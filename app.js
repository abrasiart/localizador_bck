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

/* ============================================================================
 * ARQUIVOS CSV (ajuste os nomes se mudar no repositório)
 * ========================================================================== */
const CSV_SEP = ';';

const PRODUCTS_FILE = path.join(__dirname, 'produtos.csv');
const PDVS_FILE = path.join(__dirname, 'pdvs_final.csv'); // id;nome;rua;bairro;cidade;cep;estado
const PDV_PROD_FILE = path.join(__dirname, 'pdv_produtos_filtrado_final.csv'); // produto_id;pdv_id

/* ============================================================================
 * GEOCODING (OpenCage)
 * - Cache em disco para não geocodificar o mesmo endereço toda hora
 * ========================================================================== */
const OPENCAGE_KEY = process.env.OPENCAGE_KEY || '';
const GEOCODE_CACHE_FILE = path.join(__dirname, 'geocode_cache.json');

// cache em memória: address -> { lat, lon }
let geocodeCache = Object.create(null);

// carregar cache salvo
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

// garante que não faremos 2 requisições simultâneas para o mesmo endereço
const inflightGeocoding = new Map(); // address -> Promise<{lat, lon}>

async function geocodeAddress(address) {
  const addr = String(address || '').trim();
  if (!addr) return null;

  // cache hit (memória)
  if (geocodeCache[addr]) return geocodeCache[addr];

  // já tem requisição em andamento?
  if (inflightGeocoding.has(addr)) return inflightGeocoding.get(addr);

  // sem chave: não dá pra geocodificar
  if (!OPENCAGE_KEY) {
    console.warn('OPENCAGE_KEY ausente. Não é possível geocodificar:', addr);
    return null;
  }

  const p = (async () => {
    try {
      const url = `https://api.opencagedata.com/geocode/v1/json?q=${encodeURIComponent(
        addr
      )}&key=${OPENCAGE_KEY}&limit=1&no_annotations=1&language=pt-BR`;
      const r = await fetch(url);
      if (!r.ok) {
        console.warn('OpenCage HTTP', r.status, addr);
        return null;
      }
      const j = await r.json();
      const first = j && j.results && j.results[0];
      if (!first || !first.geometry) return null;

      const out = { lat: +first.geometry.lat, lon: +first.geometry.lng };
      // salvar no cache
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

/* ============================================================================
 * HELPERS
 * ========================================================================== */
function normalizeString(s) {
  return String(s ?? '').trim();
}

function onlyDigits(s, maxLen) {
  const d = String(s ?? '').replace(/\D/g, '');
  return maxLen ? d.slice(0, maxLen) : d;
}

function calculateDistance(lat1, lon1, lat2, lon2) {
  const R = 6371;
  const dLat = ((lat2 - lat1) * Math.PI) / 180;
  const dLon = ((lon2 - lon1) * Math.PI) / 180;
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos((lat1 * Math.PI) / 180) *
      Math.cos((lat2 * Math.PI) / 180) *
      Math.sin(dLon / 2) ** 2;
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return R * c;
}

function buildAddressFromRow(row) {
  // CSV: id;nome;rua;bairro;cidade;cep;estado
  const rua = normalizeString(row.rua);
  const bairro = normalizeString(row.bairro);
  const cidade = normalizeString(row.cidade);
  const uf = normalizeString(row.estado);
  const cepRaw = normalizeString(row.cep);
  const cep = onlyDigits(cepRaw, 8);

  // Monta um endereço “amigável” para geocodificação
  const parts = [];
  if (rua) parts.push(rua);
  if (bairro) parts.push(bairro);
  if (cidade) parts.push(cidade);
  if (uf) parts.push(`${uf}, Brasil`);
  else parts.push('Brasil');
  if (cep) parts.push(cep);

  return parts.join(', ');
}

/* ============================================================================
 * CARREGAR CSVs
 * ========================================================================== */
async function loadProductsCsv() {
  return new Promise((resolve, reject) => {
    const items = [];
    fs.createReadStream(PRODUCTS_FILE)
      .pipe(csv({ separator: CSV_SEP }))
      .on('data', (raw) => {
        const r = {};
        for (const [k, v] of Object.entries(raw)) {
          r[String(k).trim().toLowerCase()] = String(v ?? '').trim();
        }
        items.push({
          id: r.id,
          nome: r.nome,
          volume: r.volume || '',
          em_destaque: ['true', '1', 'sim', 'yes'].includes(
            (r['em_destaque'] || '').toLowerCase()
          ),
          imagem_url: r['imagem_url'],
          produto_url: r['produto_url'] || null,
        });
      })
      .on('end', () => resolve(items))
      .on('error', reject);
  });
}

async function loadPdvsMap() {
  return new Promise((resolve, reject) => {
    const map = new Map();
    fs.createReadStream(PDVS_FILE)
      .pipe(csv({ separator: CSV_SEP, mapHeaders: ({ header }) => header.trim().toLowerCase() }))
      .on('data', (row) => {
        const id = normalizeString(row.id);
        if (!id) return;
        const nome = normalizeString(row.nome);
        const endereco = buildAddressFromRow(row);
        const cep = onlyDigits(row.cep, 8);
        const cidade = normalizeString(row.cidade);

        // lat/lon removidos do CSV → ficam como undefined; serão geocodificados on-demand
        map.set(id, {
          id,
          nome,
          cep,
          endereco,
          cidade,
          latitude: undefined,
          longitude: undefined,
        });
      })
      .on('end', () => resolve(map))
      .on('error', reject);
  });
}

/* ============================================================================
 * ROTAS
 * ========================================================================== */

// Healthcheck simples
app.get('/health', (_, res) => res.json({ ok: true }));

// Produtos em destaque
app.get('/produtos/destaque', async (req, res) => {
  try {
    const all = await loadProductsCsv();
    const destacados = all.filter((p) => p.em_destaque);
    res.json(destacados);
  } catch (e) {
    console.error('Erro /produtos/destaque:', e);
    res.status(500).json({ erro: 'Falha ao ler produtos.' });
  }
});

// Buscar produtos por nome
app.get('/produtos/buscar', async (req, res) => {
  try {
    const q = String(req.query.q ?? '').toLowerCase();
    if (!q) return res.status(400).json({ erro: 'Termo de busca é obrigatório.' });

    const all = await loadProductsCsv();
    const results = all.filter((p) => (p.nome || '').toLowerCase().includes(q));
    res.json(results);
  } catch (e) {
    console.error('Erro /produtos/buscar:', e);
    res.status(500).json({ erro: 'Falha ao buscar produtos.' });
  }
});

// Função auxiliar: garante que o PDV tenha latitude/longitude usando geocoding
async function ensurePdvCoords(pdv) {
  if (Number.isFinite(pdv.latitude) && Number.isFinite(pdv.longitude)) {
    return true;
  }
  const geo = await geocodeAddress(pdv.endereco);
  if (!geo) return false;
  pdv.latitude = geo.lat;
  pdv.longitude = geo.lon;
  return true;
}

// PDVs próximos por CEP → devolve “todos” os PDVs ordenados pela distância
app.get('/pdvs/proximos', async (req, res) => {
  const userCep = onlyDigits(req.query.cep, 8);
  if (userCep.length !== 8) return res.status(400).json({ erro: 'CEP inválido.' });

  try {
    // converte CEP → lat/lon (AwesomeAPI)
    const r = await fetch(`https://cep.awesomeapi.com.br/json/${userCep}`);
    const j = await r.json();
    if (!j.lat || !j.lng) return res.status(404).json({ erro: 'CEP não encontrado.' });

    const userLat = parseFloat(j.lat);
    const userLon = parseFloat(j.lng);

    const pdvMap = await loadPdvsMap();
    const out = [];

    // geocodifica cada PDV (com cache); calcula distância
    for (const pdv of pdvMap.values()) {
      const ok = await ensurePdvCoords(pdv);
      if (!ok) continue;

      const distancia = calculateDistance(userLat, userLon, pdv.latitude, pdv.longitude);
      out.push({
        id: pdv.id,
        nome: pdv.nome,
        cep: pdv.cep,
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
    res.status(500).json({ erro: 'Erro ao calcular PDVs próximos.' });
  }
});

// PDVs próximos por coordenadas (lat/lon)
app.get('/pdvs/proximos/coords', async (req, res) => {
  const userLat = parseFloat(String(req.query.lat ?? ''));
  const userLon = parseFloat(String(req.query.lon ?? ''));
  if (!Number.isFinite(userLat) || !Number.isFinite(userLon)) {
    return res.status(400).json({ erro: 'Coordenadas inválidas.' });
  }

  try {
    const pdvMap = await loadPdvsMap();
    const out = [];
    for (const pdv of pdvMap.values()) {
      const ok = await ensurePdvCoords(pdv);
      if (!ok) continue;
      const distancia = calculateDistance(userLat, userLon, pdv.latitude, pdv.longitude);
      out.push({
        id: pdv.id,
        nome: pdv.nome,
        cep: pdv.cep,
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
    res.status(500).json({ erro: e.message });
  }
});

// PDVs por produto + coordenadas do usuário
app.get(['/pdvs/proximos/produto', '/produto'], async (req, res) => {
  const productIdRaw = String(req.query.productId ?? '').trim();
  const userLat = parseFloat(String(req.query.lat ?? ''));
  const userLon = parseFloat(String(req.query.lon ?? ''));

  if (!productIdRaw || !Number.isFinite(userLat) || !Number.isFinite(userLon)) {
    return res.status(400).json({ erro: 'Parâmetros inválidos.' });
  }

  // candidatos: 9xxxx <-> 0xxxx
  const candidates = new Set([productIdRaw]);
  if (/^\d{5}$/.test(productIdRaw)) {
    if (productIdRaw.startsWith('9')) candidates.add('0' + productIdRaw.slice(1));
    if (productIdRaw.startsWith('0')) candidates.add('9' + productIdRaw.slice(1));
  }

  try {
    const pdvMap = await loadPdvsMap();
    const out = [];

    await new Promise((resolve, reject) => {
      fs.createReadStream(PDV_PROD_FILE)
        .pipe(csv({ separator: CSV_SEP, mapHeaders: ({ header }) => header.trim().toLowerCase() }))
        .on('data', (row) => {
          const pid = String(row.produto_id ?? '').trim();
          if (!candidates.has(pid)) return;

          const pdvId = String(row.pdv_id ?? row.id ?? '').trim();
          const pdv = pdvMap.get(pdvId);
          if (!pdv) return;

          // deixa para geocodificar depois (para podermos await fora do stream)
          out.push(pdv);
        })
        .on('end', () => resolve())
        .on('error', reject);
    });

    const enriched = [];
    for (const pdv of out) {
      const ok = await ensurePdvCoords(pdv);
      if (!ok) continue;
      const distancia = calculateDistance(userLat, userLon, pdv.latitude, pdv.longitude);
      enriched.push({
        id: pdv.id,
        nome: pdv.nome,
        cep: pdv.cep,
        endereco: pdv.endereco,
        latitude: pdv.latitude,
        longitude: pdv.longitude,
        distancia_km: +distancia.toFixed(2),
      });
    }

    enriched.sort((a, b) => a.distancia_km - b.distancia_km);
    res.json(enriched);
  } catch (e) {
    console.error('Erro /pdvs/proximos/produto:', e);
    res.status(500).json({ erro: e.message });
  }
});

/* ============================================================================
 * START
 * ========================================================================== */
app.listen(PORT, () => {
  console.log(`Servidor rodando na porta ${PORT}`);
});
