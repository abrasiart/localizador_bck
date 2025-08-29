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

// boolean + número
function toBool(x) {
  const v = String(x ?? '').trim().toLowerCase();
  return v === 'true' || v === '1' || v === 't' || v === 'sim' || v === 'yes';
}

function toNum(x) {
  if (x == null) return NaN;
  return parseFloat(String(x).replace(',', '.'));
}

// mapeia possíveis nomes de coluna para uma única chave canônica
function pick(obj, keys) {
  for (const k of keys) {
    if (obj[k] != null && obj[k] !== '') return obj[k];
  }
  return undefined;
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
// ---------------------------------------------------------------------
const PRODUCTS_FILE = path.join(__dirname, 'produtos.csv');
const PDVS_FILE = path.join(__dirname, 'pontos_de_venda_final.csv');
const PDV_PROD_FILE = path.join(__dirname, 'pdv_produtos_filtrado_final.csv');

// ---------------------------------------------------------------------
// Leitura dos CSVs
// ---------------------------------------------------------------------
// Leitura ROBUSTA dos produtos (normaliza cabeçalhos e valores)
function loadProductsCsv() {
  return new Promise((resolve, reject) => {
    const items = [];
    fs.createReadStream(PRODUCTS_FILE)
      .pipe(csv({
        separator: CSV_SEP,
        mapHeaders: ({ header }) => String(header).trim().toLowerCase().replace(/^\uFEFF/, ''),
        mapValues: ({ value }) => (typeof value === 'string' ? value.trim() : value),
      }))
      .on('data', (row) => {
        // aceita variações de nomes
        const id           = pick(row, ['id', 'produto_id']);
        const nome         = pick(row, ['nome', 'produto', 'produto_nome']);
        const volume       = pick(row, ['volume', 'tamanho', 'peso']) || '';
        const emDestaqueV  = pick(row, ['em_destaque', 'em destaque', 'destaque', 'em-destaque']);
        const imagemUrl    = pick(row, ['imagem_url', 'imagem', 'image_url', 'img']);
        const produtoUrl   = pick(row, ['produto_url', 'url', 'link', 'page_url']) || null;

        items.push({
          id,
          nome,
          volume,
          em_destaque: toBool(emDestaqueV),
          imagem_url: imagemUrl,
          produto_url: produtoUrl,
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
      .pipe(csv({
        separator: CSV_SEP,
        mapHeaders: ({ header }) => String(header).trim().toLowerCase().replace(/^\uFEFF/, ''),
        mapValues: ({ value }) => (typeof value === 'string' ? value.trim() : value),
      }))
      .on('data', (row) => {
        const id = String(pick(row, ['id', 'pdv_id']) ?? '').trim();
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

// Produtos em destaque (sem limite por padrão; aceita ?limit=5 se quiser)
app.get('/produtos/destaque', async (req, res) => {
  try {
    const limit = Number(req.query.limit);
    const all = await loadProductsCsv();

    let destacados = all.filter((p) => p.em_destaque === true);

    // (opcional) permitir limitar via querystring
    if (Number.isFinite(limit) && limit > 0) {
      destacados = destacados.slice(0, limit);
    }

    return res.json(destacados);
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
    const j = await r.json();
    if (!j.lat || !j.lng) return res.status(404).json({ erro: 'CEP não encontrado.' });

    const userLat = parseFloat(j.lat);
    const userLon = parseFloat(j.lng);

    const out = [];
    fs.createReadStream(PDVS_FILE)
      .pipe(csv({
        separator: CSV_SEP,
        mapHeaders: ({ header }) => String(header).trim().toLowerCase().replace(/^\uFEFF/, ''),
        mapValues: ({ value }) => (typeof value === 'string' ? value.trim() : value),
      }))
      .on('data', (row) => {
        const lat = toNum(row.latitude);
        const lon = toNum(row.longitude);
        if (isFinite(lat) && isFinite(lon)) {
          const distancia = calculateDistance(userLat, userLon, lat, lon);
          out.push({
            id: String(pick(row, ['id', 'pdv_id']) ?? '').trim(),
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
    console.error('Erro CEP:', e);
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
    .pipe(csv({
      separator: CSV_SEP,
      mapHeaders: ({ header }) => String(header).trim().toLowerCase().replace(/^\uFEFF/, ''),
      mapValues: ({ value }) => (typeof value === 'string' ? value.trim() : value),
    }))
    .on('data', (row) => {
      const lat = toNum(row.latitude);
      const lon = toNum(row.longitude);
      if (isFinite(lat) && isFinite(lon)) {
        const distancia = calculateDistance(userLat, userLon, lat, lon);
        out.push({
          id: String(pick(row, ['id', 'pdv_id']) ?? '').trim(),
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

// PDVs por produto + coordenadas
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
      .pipe(csv({
        separator: CSV_SEP,
        mapHeaders: ({ header }) => String(header).trim().toLowerCase().replace(/^\uFEFF/, ''),
        mapValues: ({ value }) => (typeof value === 'string' ? value.trim() : value),
      }))
      .on('data', (row) => {
        const pid = String(row.produto_id ?? '').trim();
        if (!candidates.has(pid)) return;

        const pdvId = String(pick(row, ['pdv_id', 'id']) ?? '').trim();
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
    console.error('Erro /pdvs/proximos/produto:', e);
    res.status(500).json({ erro: e.message });
  }
});

// Geocodificação reversa via backend (usa OPENCAGE_KEY do ambiente)
app.get('/geocode/reverse', async (req, res) => {
  const lat = String(req.query.lat ?? '').trim();
  const lon = String(req.query.lon ?? '').trim();
  const KEY = process.env.OPENCAGE_KEY;

  if (!lat || !lon) {
    return res.status(400).json({ erro: 'Parâmetros lat e lon são obrigatórios.' });
  }
  if (!KEY) {
    return res.status(500).json({ erro: 'OPENCAGE_KEY não configurada no servidor.' });
  }

  try {
    const url = `https://api.opencagedata.com/geocode/v1/json?q=${encodeURIComponent(lat)}+${encodeURIComponent(lon)}&key=${KEY}&pretty=0&no_annotations=1`;
    const r = await fetch(url);
    if (!r.ok) {
      return res.status(502).json({ erro: `OpenCage HTTP ${r.status}` });
    }
    const j = await r.json();
    const first = j?.results?.[0];

    return res.json({
      formatted: first?.formatted || `${Number(lat).toFixed(4)}, ${Number(lon).toFixed(4)}`,
      components: first?.components || null,
    });
  } catch (e) {
    console.error('Reverse geocode error:', e);
    return res.status(500).json({ erro: 'Falha ao geocodificar reverso.' });
  }
});

// ---------------------------------------------------------------------
app.listen(PORT, () => {
  console.log(`Servidor rodando na porta ${PORT}`);
});
