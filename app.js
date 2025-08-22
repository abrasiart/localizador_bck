const express = require('express');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const csv = require('csv-parser');
const fetch = require('node-fetch');

const app = express();
const PORT = process.env.PORT || 5000;

app.use(cors());
app.use(express.json());

// ---------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------
const CSV_SEP = ';';

// normaliza chaves/valores de uma linha do CSV
function normalizeRow(row) {
  const out = {};
  for (const [k, v] of Object.entries(row)) {
    let key = String(k).trim().toLowerCase();
    key = key.replace(/^\uFEFF/, ''); // remove BOM se houver
    out[key] = typeof v === 'string' ? v.trim() : v;
  }
  return out;
}

function toBool(x) {
  const v = String(x ?? '').trim().toLowerCase();
  return v === 'true' || v === '1' || v === 'sim' || v === 'yes';
}

function toNum(x) {
  if (x == null) return NaN;
  return parseFloat(String(x).replace(',', '.'));
}

function calculateDistance(lat1, lon1, lat2, lon2) {
  const R = 6371;
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLon = (lon2 - lon1) * Math.PI / 180;
  const a =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
    Math.sin(dLon / 2) * Math.sin(dLon / 2);
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return R * c;
}

// ---------------------------------------------------------------------
// Caminhos dos arquivos CSV
// (estão na raiz do projeto no Railway)
// ---------------------------------------------------------------------
const PRODUCTS_FILE = path.join(__dirname, 'produtos.csv');
const PDVS_FILE = path.join(__dirname, 'pontos_de_venda_final.csv');
const PDV_PROD_FILE = path.join(__dirname, 'pdv_produtos_filtrado_final.csv');

// ---------------------------------------------------------------------
// Leitura dos CSVs
// ---------------------------------------------------------------------
function loadProductsCsv() {
  return new Promise((resolve, reject) => {
    const items = [];
    fs.createReadStream(PRODUCTS_FILE)
      .pipe(csv({ separator: CSV_SEP }))
      .on('data', raw => {
        const r = normalizeRow(raw);
        items.push({
          id: r.id,
          nome: r.nome,
          volume: r.volume || '',
          em_destaque: toBool(r['em_destaque']),
          imagem_url: r['imagem_url'],
          produto_url: r['produto_url'] || null,
        });
      })
      .on('end', () => resolve(items))
      .on('error', reject);
  });
}

function loadPdvsMap() {
  return new Promise((resolve, reject) => {
    const map = new Map();
    fs.createReadStream(PDVS_FILE)
      .pipe(csv({ separator: CSV_SEP, mapHeaders: ({ header }) => header.trim() }))
      .on('data', (row) => {
        const id = String(row.id ?? row.pdv_id ?? '').trim();
        if (!id) return;
        const lat = toNum(row.latitude);
        const lon = toNum(row.longitude);
        map.set(id, {
          id,
          nome: (row.nome ?? '').trim(),
          cep: (row.cep ?? '').trim(),
          endereco: (row.endereco ?? '').trim(),
          latitude: lat,
          longitude: lon,
        });
      })
      .on('end', () => resolve(map))
      .on('error', reject);
  });
}

// ---------------------------------------------------------------------
// Rotas
// ---------------------------------------------------------------------

// Healthcheck
app.get('/health', (_, res) => res.json({ ok: true }));

// Produtos em destaque
app.get('/produtos/destaque', async (req, res) => {
  try {
    const all = await loadProductsCsv();
    const destacados = all.filter(p => p.em_destaque);
    return res.json(destacados); // <- AQUI estava o bug: retornava "destaques"
  } catch (e) {
    console.error('Erro /produtos/destaque:', e);
    return res.status(500).json({ erro: 'Falha ao ler produtos.' });
  }
});

// Buscar produtos por nome
app.get('/produtos/buscar', async (req, res) => {
  try {
    const q = String(req.query.q ?? '').toLowerCase();
    if (!q) return res.status(400).json({ erro: 'Termo de busca é obrigatório.' });

    const all = await loadProductsCsv();
    const results = all.filter(p => (p.nome || '').toLowerCase().includes(q));
    return res.json(results);
  } catch (e) {
    console.error('Erro /produtos/buscar:', e);
    return res.status(500).json({ erro: 'Falha ao buscar produtos.' });
  }
});

// PDVs próximos por CEP
app.get('/pdvs/proximos', async (req, res) => {
  const userCep = String(req.query.cep ?? '').replace(/\D/g, '');
  if (userCep.length !== 8) return res.status(400).json({ erro: 'CEP inválido.' });

  try {
    const r = await fetch(`https://cep.awesomeapi.com.br/json/${userCep}`);
    const j = await r.json()
