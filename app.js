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

// Arquivos
const PRODUCTS_FILE = path.join(__dirname, 'produtos.csv');
const PDVS_FILE = path.join(__dirname, 'pontos_de_venda_final.csv');
const PDV_PROD_FILE = path.join(__dirname, 'pdv_produtos_filtrado_final.csv');

// Helpers
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

function toBool(v) {
  const s = String(v ?? '').trim().toUpperCase();
  return s === 'TRUE' || s === '1' || s === 'T' || s === 'SIM';
}

function toNum(x) {
  if (x == null) return NaN;
  // CSV já está com ponto como separador decimal
  return parseFloat(String(x).replace(',', '.'));
}

// Carrega e indexa todos os PDVs (id -> dados do PDV)
function loadPdvsMap() {
  return new Promise((resolve, reject) => {
    const map = new Map();
    fs.createReadStream(PDVS_FILE)
      .pipe(csv({ separator: ';', mapHeaders: ({ header }) => header.trim() }))
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

// ---------- ROTAS ----------

// Produtos em destaque
app.get('/produtos/destaque', (req, res) => {
  const results = [];
  fs.createReadStream(PRODUCTS_FILE)
    .pipe(csv({ separator: ';', mapHeaders: ({ header }) => header.trim() }))
    .on('data', (row) => {
      if (toBool(row.em_destaque)) {
        results.push({
          id: String(row.id ?? '').trim(),
          nome: (row.nome ?? '').trim(),
          volume: (row['volume'] ?? row.vol ?? '').trim(),
          em_destaque: true,
          imagem_url: (row.imagem_url ?? row.image_url ?? row.imagem ?? '').trim(),
        });
      }
    })
    .on('end', () => res.json(results))
    .on('error', (e) => res.status(500).json({ erro: e.message }));
});

// Buscar produtos por nome
app.get('/produtos/buscar', (req, res) => {
  const q = String(req.query.q ?? '').trim().toLowerCase();
  if (!q) return res.status(400).json({ erro: 'Termo de busca é obrigatório.' });

  const results = [];
  fs.createReadStream(PRODUCTS_FILE)
    .pipe(csv({ separator: ';', mapHeaders: ({ header }) => header.trim() }))
    .on('data', (row) => {
      const nome = String(row.nome ?? '').trim();
      if (nome.toLowerCase().includes(q)) {
        results.push({
          id: String(row.id ?? '').trim(),
          nome,
          volume: (row['volume'] ?? row.vol ?? '').trim(),
          em_destaque: toBool(row.em_destaque),
          imagem_url: (row.imagem_url ?? row.image_url ?? row.imagem ?? '').trim(),
        });
      }
    })
    .on('end', () => res.json(results))
    .on('error', (e) => res.status(500).json({ erro: e.message }));
});

// PDVs próximos por CEP
app.get('/pdvs/proximos', async (req, res) => {
  const userCep = String(req.query.cep ?? '').replace(/\D/g, '');
  if (userCep.length !== 8) return res.status(400).json({ erro: 'CEP inválido.' });

  try {
    const r = await fetch(`https://cep.awesomeapi.com.br/json/${userCep}`);
    const j = await r.json();
    if (!j.lat || !j.lng) return res.status(404).json({ erro: 'CEP não encontrado.' });

    const userLat = parseFloat(j.lat);
    const userLon = parseFloat(j.lng);

    const out = [];
    fs.createReadStream(PDVS_FILE)
      .pipe(csv({ separator: ';', mapHeaders: ({ header }) => header.trim() }))
      .on('data', (row) => {
        const lat = toNum(row.latitude);
        const lon = toNum(row.longitude);
        if (isFinite(lat) && isFinite(lon)) {
          const distancia = calculateDistance(userLat, userLon, lat, lon);
          out.push({
            id: String(row.id ?? row.pdv_id ?? '').trim(),
            nome: (row.nome ?? '').trim(),
            cep: (row.cep ?? '').trim(),
            endereco: (row.endereco ?? '').trim(),
            latitude: lat,
            longitude: lon,
            distancia_km: +distancia.toFixed(2),
          });
        }
      })
      .on('end', () => {
        out.sort((a, b) => a.distancia_km - b.distancia_km);
        res.json(out);
      })
      .on('error', (e) => res.status(500).json({ erro: e.message }));
  } catch (e) {
    res.status(500).json({ erro: 'Erro ao buscar coordenadas do CEP.' });
  }
});

// PDVs próximos por coordenadas
app.get('/pdvs/proximos/coords', (req, res) => {
  const userLat = toNum(req.query.lat);
  const userLon = toNum(req.query.lon);
  if (!isFinite(userLat) || !isFinite(userLon)) {
    return res.status(400).json({ erro: 'Coordenadas inválidas.' });
  }

  const out = [];
  fs.createReadStream(PDVS_FILE)
    .pipe(csv({ separator: ';', mapHeaders: ({ header }) => header.trim() }))
    .on('data', (row) => {
      const lat = toNum(row.latitude);
      const lon = toNum(row.longitude);
      if (isFinite(lat) && isFinite(lon)) {
        const distancia = calculateDistance(userLat, userLon, lat, lon);
        out.push({
          id: String(row.id ?? row.pdv_id ?? '').trim(),
          nome: (row.nome ?? '').trim(),
          cep: (row.cep ?? '').trim(),
          endereco: (row.endereco ?? '').trim(),
          latitude: lat,
          longitude: lon,
          distancia_km: +distancia.toFixed(2),
        });
      }
    })
    .on('end', () => {
      out.sort((a, b) => a.distancia_km - b.distancia_km);
      res.json(out);
    })
    .on('error', (e) => res.status(500).json({ erro: e.message }));
});

// PDVs por produto + coordenadas (JOIN pdv_produtos -> pontos_de_venda)
app.get('/pdvs/proximos/produto', async (req, res) => {
  const productIdRaw = String(req.query.productId ?? '').trim();
  const userLat = toNum(req.query.lat);
  const userLon = toNum(req.query.lon);

  if (!productIdRaw || !isFinite(userLat) || !isFinite(userLon)) {
    return res.status(400).json({ erro: 'Parâmetros inválidos.' });
  }

  // candidatos: id enviado e variações 9xxxx <-> 0xxxx
  const candidates = new Set([productIdRaw]);
  if (/^\d{5}$/.test(productIdRaw)) {
    if (productIdRaw.startsWith('9')) candidates.add('0' + productIdRaw.slice(1));
    if (productIdRaw.startsWith('0')) candidates.add('9' + productIdRaw.slice(1));
  }

  try {
    const pdvMap = await loadPdvsMap();
    const out = [];

    fs.createReadStream(PDV_PROD_FILE)
      .pipe(csv({ separator: ';', mapHeaders: ({ header }) => header.trim() }))
      .on('data', (row) => {
        const pid = String(row.produto_id ?? '').trim();
        if (!candidates.has(pid)) return;

        const pdvId = String(row.pdv_id ?? row.id ?? '').trim();
        const pdv = pdvMap.get(pdvId);
        if (!pdv) return;

        const { latitude, longitude } = pdv;
        if (!isFinite(latitude) || !isFinite(longitude)) return;

        const distancia = calculateDistance(userLat, userLon, latitude, longitude);
        out.push({
          id: pdv.id,
          nome: pdv.nome,
          cep: pdv.cep,
          endereco: pdv.endereco,
          latitude,
          longitude,
          distancia_km: +distancia.toFixed(2),
        });
      })
      .on('end', () => {
        out.sort((a, b) => a.distancia_km - b.distancia_km);
        res.json(out);
      })
      .on('error', (e) => res.status(500).json({ erro: e.message }));
  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

app.get('/health', (_, res) => res.json({ ok: true }));

app.listen(PORT, () => {
  console.log(`Servidor rodando na porta ${PORT}`);
});
