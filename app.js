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
    key = key.replace(/^\uFEFF/, ''); // remove BOM, se houver
    out[key] = typeof v === 'string' ? v.trim() : v;
  }
  return out;
}

// booleano mais robusto
function toBool(x) {
  const v = String(x ?? '').trim().toLowerCase();
  if (!v) return false;
  return ['true', '1', 'sim', 'yes', 'y', 't', 'on'].includes(v);
}

// tenta achar o campo correto de "em destaque" mesmo se o header variar
function getBoolFromRow(row, possibleKeys) {
  for (const key of possibleKeys) {
    if (row[key] != null && String(row[key]).trim() !== '') {
      return toBool(row[key]);
    }
  }
  return false;
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
        const emDestaque = getBoolFromRow(r, [
          'em_destaque',
          'destaque',
          'em destaque',
          'em_destaque?'
        ]);
        items.push({
          id: r.id,
          nome: r.nome,
          volume: r.volume || '',
          em_destaque: emDestaque,
          imagem_url: r['imagem_url'],
          produto_url: r['produto_url'] || null,
        });
      })
      .on('end', () => resolve(items))
      .on('error', reject);
  });
}

// leitura do CSV dos PDVs em array
function readPdvsArray() {
  return new Promise((resolve, reject) => {
    const list = [];
    fs.createReadStream(PDVS_FILE)
      .pipe(csv({ separator: CSV_SEP, mapHeaders: ({ header }) => header.trim() }))
      .on('data', (row) => list.push(row))
      .on('end', () => resolve(list))
      .on('error', reject);
  });
}

// Carrega e indexa os PDVs (id -> dados do PDV) — usado na rota por produto
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
// Geocache e geocodificação (por endereço)
// ---------------------------------------------------------------------
const OPENCAGE_KEY = process.env.OPENCAGE_KEY || '';
// Em ambientes serverless (Vercel), /tmp é gravável. Se não der, cai na pasta do projeto.
const GEO_CACHE_FILE =
  (process.env.VERCEL ? path.join('/tmp', 'pdv_geocache.json') : path.join(__dirname, 'pdv_geocache.json'));

let geoCache = {};
try {
  if (fs.existsSync(GEO_CACHE_FILE)) {
    geoCache = JSON.parse(fs.readFileSync(GEO_CACHE_FILE, 'utf8'));
  }
} catch (e) {
  console.warn('Não foi possível carregar geocache; prosseguindo sem cache.', e.message);
}

function saveGeoCache() {
  try {
    fs.writeFileSync(GEO_CACHE_FILE, JSON.stringify(geoCache, null, 2));
  } catch (e) {
    console.warn('Falha ao salvar geocache:', e.message);
  }
}

function addressKeyFromRow(row) {
  const bits = [];
  if (row.endereco) bits.push(String(row.endereco).trim());
  if (row.cep) bits.push(String(row.cep).trim());
  const key = bits.join(', ').toLowerCase();
  return key || null;
}

async function geocodeAddressFreeForm(query) {
  if (!OPENCAGE_KEY) return null;
  try {
    const url = `https://api.opencagedata.com/geocode/v1/json?q=${encodeURIComponent(query)}&key=${OPENCAGE_KEY}&limit=1&language=pt&pretty=0&no_annotations=1`;
    const r = await fetch(url);
    if (!r.ok) return null;
    const j = await r.json();
    const first = j?.results?.[0];
    if (!first?.geometry) return null;
    return { lat: Number(first.geometry.lat), lon: Number(first.geometry.lng) };
  } catch (e) {
    return null;
  }
}

// geocodifica um PDV (linha do CSV) se necessário, usando o cache
async function geocodeRowIfNeeded(row) {
  const lat = toNum(row.latitude);
  const lon = toNum(row.longitude);
  if (isFinite(lat) && isFinite(lon)) {
    return { lat, lon };
  }
  // construir a query; quanto mais contexto, melhor
  const parts = [];
  if (row.endereco) parts.push(String(row.endereco).trim());
  if (row.cep) parts.push(String(row.cep).trim());
  parts.push('Brasil');
  const query = parts.filter(Boolean).join(', ');
  if (!query) return null;

  const key = query.toLowerCase();

  if (geoCache[key]) {
    return geoCache[key];
  }

  const coords = await geocodeAddressFreeForm(query);
  if (coords && isFinite(coords.lat) && isFinite(coords.lon)) {
    geoCache[key] = coords;
    saveGeoCache();
    return coords;
  }
  return null;
}

// ---------------------------------------------------------------------
// Rotas
// ---------------------------------------------------------------------

// Healthcheck
app.get('/health', (_, res) => res.json({ ok: true }));

// Produtos em destaque (com preenchimento até "limit")
app.get('/produtos/destaque', async (req, res) => {
  try {
    const limit = Math.max(1, parseInt(req.query.limit, 10) || 5);
    const all = await loadProductsCsv();

    let destacados = all.filter(p => p.em_destaque);
    if (destacados.length < limit) {
      const faltam = limit - destacados.length;
      const complementares = all
        .filter(p => !p.em_destaque && !destacados.find(d => d.id === p.id))
        .slice(0, faltam);
      destacados = destacados.concat(complementares);
    }
    return res.json(destacados.slice(0, limit));
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

// PDVs próximos por CEP (geocodifica PDV por endereço, se faltar lat/lon)
app.get('/pdvs/proximos', async (req, res) => {
  const userCep = String(req.query.cep ?? '').replace(/\D/g, '');
  if (userCep.length !== 8) return res.status(400).json({ erro: 'CEP inválido.' });

  try {
    const r = await fetch(`https://cep.awesomeapi.com.br/json/${userCep}`);
    const j = await r.json();
    if (!j.lat || !j.lng) return res.status(404).json({ erro: 'CEP não encontrado.' });

    const userLat = parseFloat(j.lat);
    const userLon = parseFloat(j.lng);

    const rows = await readPdvsArray();
    const out = [];
    // processa sequencialmente (evita estouro de limite do geocoding)
    for (const row of rows) {
      let lat = toNum(row.latitude);
      let lon = toNum(row.longitude);
      if (!isFinite(lat) || !isFinite(lon)) {
        const coords = await geocodeRowIfNeeded(row);
        if (!coords) continue; // se não conseguir geocodificar, pula
        lat = coords.lat;
        lon = coords.lon;
      }
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

    out.sort((a, b) => a.distancia_km - b.distancia_km);
    res.json(out);
  } catch (e) {
    console.error('Erro /pdvs/proximos:', e);
    res.status(500).json({ erro: 'Erro ao processar PDVs.' });
  }
});

// PDVs próximos por coordenadas (geocodifica PDV por endereço, se faltar lat/lon)
app.get('/pdvs/proximos/coords', async (req, res) => {
  const userLat = toNum(req.query.lat);
  const userLon = toNum(req.query.lon);
  if (!isFinite(userLat) || !isFinite(userLon)) {
    return res.status(400).json({ erro: 'Coordenadas inválidas.' });
  }

  try {
    const rows = await readPdvsArray();
    const out = [];
    for (const row of rows) {
      let lat = toNum(row.latitude);
      let lon = toNum(row.longitude);
      if (!isFinite(lat) || !isFinite(lon)) {
        const coords = await geocodeRowIfNeeded(row);
        if (!coords) continue;
        lat = coords.lat;
        lon = coords.lon;
      }
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
    out.sort((a, b) => a.distancia_km - b.distancia_km);
    res.json(out);
  } catch (e) {
    console.error('Erro /pdvs/proximos/coords:', e);
    res.status(500).json({ erro: 'Erro ao processar PDVs.' });
  }
});

// PDVs por produto + coordenadas (geocodifica PDV por endereço, se faltar lat/lon)
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

    // varremos o vínculo PDV x produto e puxamos os PDVs
    const rows = await new Promise((resolve, reject) => {
      const list = [];
      fs.createReadStream(PDV_PROD_FILE)
        .pipe(csv({ separator: CSV_SEP, mapHeaders: ({ header }) => header.trim() }))
        .on('data', (row) => list.push(row))
        .on('end', () => resolve(list))
        .on('error', reject);
    });

    for (const row of rows) {
      const pid = String(row.produto_id ?? '').trim();
      if (!candidates.has(pid)) continue;

      const pdvId = String(row.pdv_id ?? row.id ?? '').trim();
      const pdv = pdvMap.get(pdvId);
      if (!pdv) continue;

      let { latitude, longitude } = pdv;
      if (!isFinite(latitude) || !isFinite(longitude)) {
        // geocodificar este PDV pela primeira vez
        const coords = await geocodeRowIfNeeded({
          endereco: pdv.endereco,
          cep: pdv.cep,
          latitude: pdv.latitude,
          longitude: pdv.longitude
        });
        if (!coords) continue;
        latitude = coords.lat;
        longitude = coords.lon;
        // atualiza o map em memória para chamadas subsequentes
        pdv.latitude = latitude;
        pdv.longitude = longitude;
      }

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
    }

    out.sort((a, b) => a.distancia_km - b.distancia_km);
    res.json(out);
  } catch (e) {
    console.error('Erro /pdvs/proximos/produto:', e);
    res.status(500).json({ erro: e.message });
  }
});

// Geocodificação reversa (continua igual)
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
