/* eslint-disable no-console */
const express = require('express');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const csv = require('csv-parser');
const fetch = require('node-fetch'); // v2

const app = express();
const PORT = process.env.PORT || 5000;

app.use(cors());
app.use(express.json());

// ======================== Config & arquivos ========================
const CSV_SEP = ';';
const PRODUCTS_FILE = path.join(__dirname, 'produtos.csv');
const PDVS_FILE = path.join(__dirname, 'pdvs_final.csv'); // id;nome;rua;bairro;cidade;cep;estado
const PDV_PROD_FILE = path.join(__dirname, 'pdv_produtos_filtrado_final.csv'); // produto_id;pdv_id

// ======================== Geocoding (OpenCage) =====================
const OPENCAGE_KEY = process.env.OPENCAGE_KEY || '';
const GEOCODE_CACHE_FILE = path.join(__dirname, 'geocode_cache.json');

let geocodeCache = Object.create(null);
try {
  if (fs.existsSync(GEOCODE_CACHE_FILE)) {
    geocodeCache = JSON.parse(fs.readFileSync(GEOCODE_CACHE_FILE, 'utf-8'));
  }
} catch (err) {
  console.warn('Não foi possível carregar geocode_cache.json:', err.message);
  geocodeCache = Object.create(null);
}
function saveGeocodeCache() {
  try {
    fs.writeFileSync(GEOCODE_CACHE_FILE, JSON.stringify(geocodeCache, null, 2), 'utf-8');
  } catch (err) {
    console.warn('Falha ao salvar geocode_cache.json:', err.message);
  }
}
const inflightGeocoding = new Map(); // address -> Promise<{lat,lon}>

async function geocodeAddress(address) {
  const addr = String(address || '').trim();
  if (!addr) return null;
  if (geocodeCache[addr]) return geocodeCache[addr];
  if (inflightGeocoding.has(addr)) return inflightGeocoding.get(addr);
  if (!OPENCAGE_KEY) {
    console.warn('OPENCAGE_KEY ausente — não dá para geocodificar:', addr);
    return null;
  }
  const p = (async () => {
    try {
      const url = `https://api.opencagedata.com/geocode/v1/json?q=${encodeURIComponent(
        addr
      )}&key=${OPENCAGE_KEY}&limit=1&no_annotations=1&language=pt-BR`;
      const r = await fetch(url);
      if (!r.ok) return null;
      const j = await r.json();
      const first = j?.results?.[0];
      if (!first?.geometry) return null;
      const out = { lat: +first.geometry.lat, lon: +first.geometry.lng };
      geocodeCache[addr] = out;
      saveGeocodeCache();
      return out;
    } catch (err) {
      console.warn('Erro geocodificando', addr, err.message);
      return null;
    } finally {
      inflightGeocoding.delete(addr);
    }
  })();
  inflightGeocoding.set(addr, p);
  return p;
}

// ======================== Helpers ========================
function norm(s) { return String(s ?? '').trim(); }
function onlyDigits(s, max) {
  const d = String(s ?? '').replace(/\D/g, '');
  return max ? d.slice(0, max) : d;
}
function distance(lat1, lon1, lat2, lon2) {
  const R = 6371;
  const dLat = ((lat2 - lat1) * Math.PI) / 180;
  const dLon = ((lon2 - lon1) * Math.PI) / 180;
  const a = Math.sin(dLat/2)**2 +
            Math.cos(lat1*Math.PI/180) * Math.cos(lat2*Math.PI/180) *
            Math.sin(dLon/2)**2;
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
  return R*c;
}
function buildAddressFromRow(row) {
  // CSV: id;nome;rua;bairro;cidade;cep;estado
  const rua = norm(row.rua);
  const bairro = norm(row.bairro);
  const cidade = norm(row.cidade);
  const uf = norm(row.estado);
  const cep = onlyDigits(row.cep, 8);

  const parts = [];
  if (rua) parts.push(rua);
  if (bairro) parts.push(bairro);
  if (cidade) parts.push(cidade);
  if (uf) parts.push(`${uf}, Brasil`); else parts.push('Brasil');
  if (cep) parts.push(cep);
  return parts.join(', ');
}

// ======================== Carregamento CSV ========================
function loadProductsCsv() {
  return new Promise((resolve, reject) => {
    const items = [];
    fs.createReadStream(PRODUCTS_FILE)
      .pipe(csv({ separator: CSV_SEP }))
      .on('data', (raw) => {
        const r = {};
        for (const [k,v] of Object.entries(raw)) r[String(k).trim().toLowerCase()] = String(v??'').trim();
        items.push({
          id: r.id,
          nome: r.nome,
          volume: r.volume || '',
          em_destaque: ['true','1','sim','yes'].includes((r['em_destaque']||'').toLowerCase()),
          imagem_url: r['imagem_url'],
          produto_url: r['produto_url'] || null
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
      .pipe(csv({ separator: CSV_SEP, mapHeaders: ({header}) => header.trim().toLowerCase() }))
      .on('data', (row) => {
        const id = norm(row.id);
        if (!id) return;
        const nome = norm(row.nome);
        const endereco = buildAddressFromRow(row);
        const cep = onlyDigits(row.cep, 8);
        const cidade = norm(row.cidade);
        map.set(id, { id, nome, cep, endereco, cidade, latitude: undefined, longitude: undefined });
      })
      .on('end', () => resolve(map))
      .on('error', reject);
  });
}

// garante que o PDV tem coords
async function ensureCoords(pdv) {
  if (Number.isFinite(pdv.latitude) && Number.isFinite(pdv.longitude)) return true;
  const geo = await geocodeAddress(pdv.endereco);
  if (!geo) return false;
  pdv.latitude = geo.lat;
  pdv.longitude = geo.lon;
  return true;
}

// ======================== Rotas ========================
app.get('/health', (_,res) => res.json({ok:true}));

app.get('/produtos/destaque', async (req,res) => {
  try {
    const all = await loadProductsCsv();
    res.json(all.filter(p => p.em_destaque));
  } catch (e) {
    console.error('Erro /produtos/destaque:', e);
    res.status(500).json({ erro: 'Falha ao ler produtos.' });
  }
});

app.get('/produtos/buscar', async (req,res) => {
  try {
    const q = String(req.query.q ?? '').toLowerCase();
    if (!q) return res.status(400).json({ erro: 'Termo de busca é obrigatório.' });
    const all = await loadProductsCsv();
    res.json(all.filter(p => (p.nome||'').toLowerCase().includes(q)));
  } catch (e) {
    console.error('Erro /produtos/buscar:', e);
    res.status(500).json({ erro: 'Falha ao buscar produtos.' });
  }
});

// PDVs por CEP
app.get('/pdvs/proximos', async (req,res) => {
  const userCep = onlyDigits(req.query.cep, 8);
  if (userCep.length !== 8) return res.status(400).json({ erro:'CEP inválido.' });
  try {
    const r = await fetch(`https://cep.awesomeapi.com.br/json/${userCep}`);
    const j = await r.json();
    if (!j.lat || !j.lng) return res.status(404).json({ erro:'CEP não encontrado.' });
    const userLat = +j.lat, userLon = +j.lng;

    const pdvMap = await loadPdvsMap();
    const out = [];
    for (const pdv of pdvMap.values()) {
      const ok = await ensureCoords(pdv);
      if (!ok) continue;
      const d = distance(userLat, userLon, pdv.latitude, pdv.longitude);
      out.push({
        id: pdv.id, nome: pdv.nome, cep: pdv.cep, endereco: pdv.endereco,
        latitude: pdv.latitude, longitude: pdv.longitude, distancia_km: +d.toFixed(2)
      });
    }
    out.sort((a,b) => a.distancia_km - b.distancia_km);
    res.json(out);
  } catch (e) {
    console.error('Erro /pdvs/proximos:', e);
    res.status(500).json({ erro:'Erro ao calcular PDVs próximos.' });
  }
});

// PDVs por coords
app.get('/pdvs/proximos/coords', async (req,res) => {
  const userLat = +String(req.query.lat ?? '');
  const userLon = +String(req.query.lon ?? '');
  if (!Number.isFinite(userLat) || !Number.isFinite(userLon)) {
    return res.status(400).json({ erro:'Coordenadas inválidas.' });
  }
  try {
    const pdvMap = await loadPdvsMap();
    const out = [];
    for (const pdv of pdvMap.values()) {
      const ok = await ensureCoords(pdv);
      if (!ok) continue;
      const d = distance(userLat, userLon, pdv.latitude, pdv.longitude);
      out.push({
        id: pdv.id, nome: pdv.nome, cep: pdv.cep, endereco: pdv.endereco,
        latitude: pdv.latitude, longitude: pdv.longitude, distancia_km: +d.toFixed(2)
      });
    }
    out.sort((a,b) => a.distancia_km - b.distancia_km);
    res.json(out);
  } catch (e) {
    console.error('Erro /pdvs/proximos/coords:', e);
    res.status(500).json({ erro:e.message });
  }
});

// PDVs por produto + coords (e alias /produto)
app.get(['/pdvs/proximos/produto', '/produto'], async (req,res) => {
  const productIdRaw = String(req.query.productId ?? '').trim();
  const userLat = +String(req.query.lat ?? '');
  const userLon = +String(req.query.lon ?? '');
  if (!productIdRaw || !Number.isFinite(userLat) || !Number.isFinite(userLon)) {
    return res.status(400).json({ erro:'Parâmetros inválidos.' });
  }
  const candidates = new Set([productIdRaw]);
  if (/^\d{5}$/.test(productIdRaw)) {
    if (productIdRaw.startsWith('9')) candidates.add('0' + productIdRaw.slice(1));
    if (productIdRaw.startsWith('0')) candidates.add('9' + productIdRaw.slice(1));
  }
  try {
    const pdvMap = await loadPdvsMap();
    const bucket = [];
    await new Promise((resolve,reject) => {
      fs.createReadStream(PDV_PROD_FILE)
        .pipe(csv({ separator: CSV_SEP, mapHeaders: ({header}) => header.trim().toLowerCase() }))
        .on('data', (row) => {
          const pid = String(row.produto_id ?? '').trim();
          if (!candidates.has(pid)) return;
          const pdvId = String(row.pdv_id ?? row.id ?? '').trim();
          const pdv = pdvMap.get(pdvId);
          if (pdv) bucket.push(pdv);
        })
        .on('end', resolve)
        .on('error', reject);
    });
    const out = [];
    for (const pdv of bucket) {
      const ok = await ensureCoords(pdv);
      if (!ok) continue;
      const d = distance(userLat, userLon, pdv.latitude, pdv.longitude);
      out.push({
        id: pdv.id, nome: pdv.nome, cep: pdv.cep, endereco: pdv.endereco,
        latitude: pdv.latitude, longitude: pdv.longitude, distancia_km: +d.toFixed(2)
      });
    }
    out.sort((a,b) => a.distancia_km - b.distancia_km);
    res.json(out);
  } catch (e) {
    console.error('Erro /pdvs/proximos/produto:', e);
    res.status(500).json({ erro:e.message });
  }
});

app.listen(PORT, () => {
  console.log(`Servidor rodando na porta ${PORT}`);
});
