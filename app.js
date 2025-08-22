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
    res.status(500).json({ erro: 'Falha ao ler produtos.csv' });
  }
});

// destaques (normalizado + fallback se vazio)
app.get('/produtos/destaque', async (_req, res) => {
  try {
    const produtos = await loadAllProducts();
    const destacados = produtos.filter((p) => p.em_destaque);
    if (destacados.length > 0) return res.json(destacados.slice(0, 12));
    // fallback: primeiros 12 para não ficar vazio
    return res.json(produtos.slice(0, 12));
  } catch (e) {
    console.error(e);
    res.status(500).json({ erro: 'Falha ao ler produtos.csv' });
  }
});

// busca por nome (case/acentos-insensível)
app.get('/produtos/buscar', async (req, res) => {
  try {
    const qRaw = String(req.query.q || '').trim();
    if (!qRaw) return res.status(400).json({ erro: 'Termo de busca é obrigatório.' });

    const query = norm(qRaw);
    const produtos = await loadAllProducts();
    const out = produtos.filter((p) => norm(p.nome).includes(query)).slice(0, 50);
    res.json(out);
  } catch (e) {
    console.error(e);
    res.status(500).json({ erro: 'Falha ao buscar produtos.' });
  }
});

// ===============================================
// Rotas de PDV (usando o mesmo parser CSV_OPTS)
// ===============================================

// PDVs por CEP
app.get('/pdvs/proximos', async (req, res) => {
  const userCep = req.query.cep;
  if (!userCep) return res.status(400).json({ erro: 'CEP é obrigatório.' });

  const cleanCep = String(userCep).replace(/\D/g, '');
  if (cleanCep.length !== 8) return res.status(400).json({ erro: 'CEP inválido.' });

  try {
    const response = await fetch(`https://cep.awesomeapi.com.br/json/${cleanCep}`);
    const data = await response.json();

    if (!data.lat || !data.lng) return res.status(404).json({ erro: 'CEP não encontrado.' });

    const userLat = parseFloat(data.lat);
    const userLon = parseFloat(data.lng);

    const pdvs = [];
    fs.createReadStream(path.join(__dirname, 'pontos_de_venda_final.csv'))
      .pipe(csv(CSV_OPTS))
      .on('data', (pdv) => {
        const pdvLat = parseFloat(pdv.latitude);
        const pdvLon = parseFloat(pdv.longitude);
        if (!isNaN(pdvLat) && !isNaN(pdvLon)) {
          const distancia = calculateDistance(userLat, userLon, pdvLat, pdvLon);
          pdvs.push({ ...pdv, distancia_km: parseFloat(distancia.toFixed(2)) });
        }
      })
      .on('end', () => {
        pdvs.sort((a, b) => a.distancia_km - b.distancia_km);
        res.json(pdvs);
      })
      .on('error', () => res.status(500).json({ erro: 'Falha ao ler pontos_de_venda_final.csv' }));
  } catch (error) {
    console.error(error);
    res.status(500).json({ erro: 'Erro ao buscar coordenadas do CEP.' });
  }
});

// PDVs por coordenadas
app.get('/pdvs/proximos/coords', (req, res) => {
  const userLat = parseFloat(req.query.lat);
  const userLon = parseFloat(req.query.lon);
  if (isNaN(userLat) || isNaN(userLon)) return res.status(400).json({ erro: 'Coordenadas inválidas.' });

  const pdvs = [];
  fs.createReadStream(path.join(__dirname, 'pontos_de_venda_final.csv'))
    .pipe(csv(CSV_OPTS))
    .on('data', (pdv) => {
      const pdvLat = parseFloat(pdv.latitude);
      const pdvLon = parseFloat(pdv.longitude);
      if (!isNaN(pdvLat) && !isNaN(pdvLon)) {
        const distancia = calculateDistance(userLat, userLon, pdvLat, pdvLon);
        pdvs.push({ ...pdv, distancia_km: parseFloat(distancia.toFixed(2)) });
      }
    })
    .on('end', () => {
      pdvs.sort((a, b) => a.distancia_km - b.distancia_km);
      res.json(pdvs);
    })
    .on('error', () => res.status(500).json({ erro: 'Falha ao ler pontos_de_venda_final.csv' }));
});

// PDVs por produto + localização (tolera várias colunas de id)
app.get('/pdvs/proximos/produto', (req, res) => {
  const { productId, lat, lon } = req.query;
  const userLat = parseFloat(lat);
  const userLon = parseFloat(lon);
  if (!productId || isNaN(userLat) || isNaN(userLon)) {
    return res.status(400).json({ erro: 'Parâmetros inválidos.' });
  }

  const target = String(productId);
  const pdvs = [];
  fs.createReadStream(path.join(__dirname, 'pdv_produtos_filtrado_final.csv'))
    .pipe(csv(CSV_OPTS))
    .on('data', (row) => {
      const idCand = [row.produto_id, row.product_id, row.productId, row.id]
        .map((v) => (v != null ? String(v) : ''));
      const sameId = idCand.includes(target);

      if (sameId) {
        const pdvLat = parseFloat(row.latitude);
        const pdvLon = parseFloat(row.longitude);
        if (!isNaN(pdvLat) && !isNaN(pdvLon)) {
          const distancia = calculateDistance(userLat, userLon, pdvLat, pdvLon);
          pdvs.push({ ...row, distancia_km: parseFloat(distancia.toFixed(2)) });
        }
      }
    })
    .on('end', () => {
      pdvs.sort((a, b) => a.distancia_km - b.distancia_km);
      res.json(pdvs);
    })
    .on('error', () => res.status(500).json({ erro: 'Falha ao ler pdv_produtos_filtrado_final.csv' }));
});

// ===============================================

app.listen(PORT, () => {
  console.log(`Servidor rodando na porta ${PORT}`);
});
