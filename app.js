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

// ---------------------------------------------------------
// Arquivos e Constantes
// ---------------------------------------------------------
const CSV_SEP = ';';

const PRODUCTS_FILE  = path.join(__dirname, 'produtos.csv');
const PDVS_FILE      = path.join(__dirname, 'pdvs_final.csv'); // <<< SEU CSV NOVO: id;nome;rua;bairro;cidade;cep;estado
const PDV_PROD_FILE  = path.join(__dirname, 'pdv_produtos_filtrado_final.csv');

const PDV_GEOCACHE_FILE = path.join(__dirname, 'pdv_geocache.json'); // cache com lat/lon por PDV

// ---------------------------------------------------------
// Utils
// ---------------------------------------------------------
function normalizeRow(row) {
  const out = {};
  for (const [k, v] of Object.entries(row)) {
    let key = String(k).trim().toLowerCase().replace(/^\uFEFF/, '');
    out[key] = typeof v === 'string' ? v.trim() : v;
  }
  return out;
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
    Math.sin(dLat / 2) ** 2 +
    Math.cos(lat1 * Math.PI / 180) *
      Math.cos(lat2 * Math.PI / 180) *
      Math.sin(dLon / 2) ** 2;
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return R * c;
}

function safeJoin(...parts) {
  return parts.filter(Boolean).map(s => String(s).trim()).join(', ');
}

function buildAddress(row) {
  // CSV: id;nome;rua;bairro;cidade;cep;estado
  const rua    = row.rua || '';
  const bairro = row.bairro || '';
  const cidade = row.cidade || '';
  const uf     = row.estado || '';
  const cep    = (row.cep || '').replace(/\D/g, '');

  // Ex.: "AVENIDA SETE DE SETEMBRO, 300, CENTRO, ITAJAÍ - SC, Brasil, 88301200"
  // (Cep no fim ajuda a geocodificar)
  const end = `${safeJoin(rua, bairro, cidade)} - ${uf}, Brasil${cep ? ', ' + cep : ''}`;
  return end;
}

// ---------------------------------------------------------
// Produtos (igual você já tinha)
// ---------------------------------------------------------
function toBool(x) {
  const v = String(x ?? '').trim().toLowerCase();
  return v === 'true' || v === '1' || v === 'sim' || v === 'yes';
}

function loadProductsCsv() {
  return new Promise((resolve, reject) => {
    const items = [];
    fs.createReadStream(PRODUCTS_FILE)
      .pipe(csv({ separator: CSV_SEP }))
      .on('data', (raw) => {
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

// ---------------------------------------------------------
// Geocodificação (OpenCage) + Cache
// ---------------------------------------------------------
function readGeocache() {
  try {
    if (fs.existsSync(PDV_GEOCACHE_FILE)) {
      const raw = fs.readFileSync(PDV_GEOCACHE_FILE, 'utf-8');
      return JSON.parse(raw || '{}');
    }
  } catch (e) {
    console.warn('Não foi possível ler pdv_geocache.json:', e.message);
  }
  return {};
}

function writeGeocache(obj) {
  try {
    fs.writeFileSync(PDV_GEOCACHE_FILE, JSON.stringify(obj, null, 2), 'utf-8');
  } catch (e) {
    console.warn('Não foi possível gravar pdv_geocache.json:', e.message);
  }
}

async function geocodeAddress(address) {
  const KEY = process.env.OPENCAGE_KEY;
  if (!KEY) throw new Error('OPENCAGE_KEY não configurada.');

  const url = `https://api.opencagedata.com/geocode/v1/json?q=${encodeURIComponent(address)}&key=${KEY}&pretty=0&no_annotations=1&countrycode=br`;
  const r = await fetch(url);
  if (!r.ok) throw new Error(`OpenCage HTTP ${r.status}`);
  const j = await r.json();
  const first = j?.results?.[0];
  if (!first?.geometry) return null;

  return {
    lat: Number(first.geometry.lat),
    lon: Number(first.geometry.lng),
    formatted: first.formatted || address,
  };
}

// ---------------------------------------------------------
// Carrega PDVs (CSV novo) e garante lat/lon via geocode + cache
// Retorna Map id -> {id, nome, cep, endereco, latitude, longitude}
// ---------------------------------------------------------
async function loadPdvsMap() {
  const cache = readGeocache();
  const map = new Map();

  const rows = [];
  await new Promise((resolve, reject) => {
    fs.createReadStream(PDVS_FILE)
      .pipe(csv({ separator: CSV_SEP }))
      .on('data', (raw) => rows.push(normalizeRow(raw)))
      .on('end', resolve)
      .on('error', reject);
  });

  let dirty = false;

  for (const row of rows) {
    const id = String(row.id || '').trim();
    if (!id) continue;

    const nome = row.nome || '';
    const endereco = buildAddress(row); // monta o endereço completo
    const cep = (row.cep || '').replace(/\D/g, '');

    // tenta cache:
    let lat = null, lon = null;
    if (cache[id] && typeof cache[id].lat === 'number' && typeof cache[id].lon === 'number') {
      lat = cache[id].lat;
      lon = cache[id].lon;
    } else {
      // geocodifica e cacheia
      try {
        const geo = await geocodeAddress(endereco);
        if (geo && Number.isFinite(geo.lat) && Number.isFinite(geo.lon)) {
          lat = geo.lat; lon = geo.lon;
          cache[id] = { lat, lon, endereco_formatado: geo.formatted || endereco };
          dirty = true;
          // pequena pausa para não estourar cota (opcional)
          await new Promise(r => setTimeout(r, 150));
        } else {
          console.warn(`Geocode falhou para PDV ${id} (${nome}). Endereço: ${endereco}`);
        }
      } catch (e) {
        console.warn(`Erro geocodificando PDV ${id} (${nome}):`, e.message);
      }
    }

    // Só adiciona PDV se conseguiu coordenadas
    if (Number.isFinite(lat) && Number.isFinite(lon)) {
      map.set(id, {
        id,
        nome,
        cep,
        endereco,       // string montada (útil para debug)
        latitude: lat,
        longitude: lon,
      });
    }
  }

  if (dirty) writeGeocache(cache);
  return map;
}

// ---------------------------------------------------------
// Rotas
// ---------------------------------------------------------

// Healthcheck
app.get('/health', (_, res) => res.json({ ok: true }));

// Produtos em destaque
app.get('/produtos/destaque', async (req, res) => {
  try {
    const all = await loadProductsCsv();
    const destacados = all.filter(p => p.em_destaque);
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

// PDVs próximos por CEP (do usuário)
app.get('/pdvs/proximos', async (req, res) => {
  const userCep = String(req.query.cep ?? '').replace(/\D/g, '');
  if (userCep.length !== 8) return res.status(400).json({ erro: 'CEP inválido.' });

  try {
    // converte CEP do usuário em lat/lon
    const r = await fetch(`https://cep.awesomeapi.com.br/json/${userCep}`);
    const j = await r.json();
    if (!j.lat || !j.lng) return res.status(404).json({ erro: 'CEP não encontrado.' });

    const userLat = parseFloat(j.lat);
    const userLon = parseFloat(j.lng);

    // carrega PDVs (geocodificados via cache)
    const pdvMap = await loadPdvsMap();

    const out = [];
    for (const pdv of pdvMap.values()) {
      const distancia = calculateDistance(userLat, userLon, pdv.latitude, pdv.longitude);
      out.push({
        id: pdv.id,
        nome: pdv.nome,
        cep: pdv.cep || '',
        endereco: pdv.endereco,
        latitude: pdv.latitude,
        longitude: pdv.longitude,
        distancia_km: +distancia.toFixed(2),
      });
    }
    out.sort((a, b) => a.distancia_km - b.distancia_km);
    res.json(out);
  } catch (e) {
    console.error('Erro /pdvs/proximos:', e);
    res.status(500).json({ erro: 'Falha ao buscar PDVs.' });
  }
});

// PDVs próximos por coordenadas (do usuário)
app.get('/pdvs/proximos/coords', async (req, res) => {
  const userLat = toNum(req.query.lat);
  const userLon = toNum(req.query.lon);
  if (!isFinite(userLat) || !isFinite(userLon)) {
    return res.status(400).json({ erro: 'Coordenadas inválidas.' });
  }

  try {
    const pdvMap = await loadPdvsMap();

    const out = [];
    for (const pdv of pdvMap.values()) {
      const distancia = calculateDistance(userLat, userLon, pdv.latitude, pdv.longitude);
      out.push({
        id: pdv.id,
        nome: pdv.nome,
        cep: pdv.cep || '',
        endereco: pdv.endereco,
        latitude: pdv.latitude,
        longitude: pdv.longitude,
        distancia_km: +distancia.toFixed(2),
      });
    }
    out.sort((a, b) => a.distancia_km - b.distancia_km);
    res.json(out);
  } catch (e) {
    console.error('Erro /pdvs/proximos/coords:', e);
    res.status(500).json({ erro: 'Falha ao buscar PDVs.' });
  }
});

// PDVs por produto + coordenadas do usuário
app.get('/pdvs/proximos/produto', async (req, res) => {
  const productIdRaw = String(req.query.productId ?? '').trim();
  const userLat = toNum(req.query.lat);
  const userLon = toNum(req.query.lon);

  if (!productIdRaw || !isFinite(userLat) || !isFinite(userLon)) {
    return res.status(400).json({ erro: 'Parâmetros inválidos.' });
  }

  // candidatos 9xxxx <-> 0xxxx
  const candidates = new Set([productIdRaw]);
  if (/^\d{5}$/.test(productIdRaw)) {
    if (productIdRaw.startsWith('9')) candidates.add('0' + productIdRaw.slice(1));
    if (productIdRaw.startsWith('0')) candidates.add('9' + productIdRaw.slice(1));
  }

  try {
    const pdvMap = await loadPdvsMap(); // já com lat/lon via geocode
    const out = [];

    fs.createReadStream(PDV_PROD_FILE)
      .pipe(csv({ separator: CSV_SEP }))
      .on('data', (row) => {
        const prodId = String(row.produto_id ?? '').trim();
        if (!candidates.has(prodId)) return;

        const pdvId = String(row.pdv_id ?? row.id ?? '').trim();
        const pdv = pdvMap.get(pdvId);
        if (!pdv) return;

        const distancia = calculateDistance(userLat, userLon, pdv.latitude, pdv.longitude);
        out.push({
          id: pdv.id,
          nome: pdv.nome,
          cep: pdv.cep || '',
          endereco: pdv.endereco,
          latitude: pdv.latitude,
          longitude: pdv.longitude,
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

// Geocodificação reversa (endereço do usuário) — permanece igual
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

// (Opcional) endpoint para limpar cache – proteja se for expor
app.post('/admin/clear-pdv-cache', (req, res) => {
  try {
    if (fs.existsSync(PDV_GEOCACHE_FILE)) fs.unlinkSync(PDV_GEOCACHE_FILE);
    return res.json({ ok: true, msg: 'Cache removido.' });
  } catch (e) {
    return res.status(500).json({ ok: false, erro: e.message });
  }
});

// ---------------------------------------------------------
app.listen(PORT, () => {
  console.log(`Servidor rodando na porta ${PORT}`);
});
