// app.js
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

// ===============================================
// Helpers
// ===============================================
const CSV_OPTS = {
  separator: ';',
  mapValues: ({ value }) => (typeof value === 'string' ? value.trim() : value),
};

function toBool(v) {
  if (typeof v === 'boolean') return v;
  const s = String(v ?? '').trim().toUpperCase();
  return s === 'TRUE' || s === '1' || s === 'SIM' || s === 'YES' || s === 'Y';
}

function normalizeProduct(row) {
  return {
    id: row.id || row.produto_id || row.product_id || row.codigo || row.sku,
    nome: row.nome || row.name || row.produto || '',
    volume: row.volume || row.tamanho || '',
    em_destaque: toBool(row.em_destaque || row.destaque || row.highlight),
    imagem_url: row.imagem_url || row.image_url || row.imagem || row.foto_url || '',
  };
}

function loadAllProducts() {
  return new Promise((resolve, reject) => {
    const arr = [];
    fs.createReadStream(path.join(__dirname, 'produtos.csv'))
      .pipe(csv(CSV_OPTS))
      .on('data', (row) => arr.push(normalizeProduct(row)))
      .on('end', () => resolve(arr))
      .on('error', reject);
  });
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

const norm = (s) =>
  String(s || '').normalize('NFD').replace(/\p{Diacritic}/gu, '').toLowerCase();

// ===============================================
// Rotas de Produtos
// ===============================================

// (opcional) health
app.get('/health', (_req, res) => res.json({ ok: true }));

// lista todos (útil para testes/fallback)
app.get('/produtos', async (_req, res) => {
  try {
    const produtos = await loadAllProducts();
    res.json(produtos);
  } catch (e) {
    console.error(e);
    res.status(500).
