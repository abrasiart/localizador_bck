// app.js
const express = require('express');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const csv = require('csv-parser');

// IMPORTANTE: use node-fetch v2 no package.json
const fetch = require('node-fetch'); // v2.x

const app = express();
const PORT = process.env.PORT || 5000;

app.use(cors());
app.use(express.json());

// ---------------------------------------------------------------------
// Logs de segurança para crash não silencioso
// ---------------------------------------------------------------------
process.on('uncaughtException', (err) => {
  console.error('[FATAL] uncaughtException:', err);
});
process.on('unhandledRejection', (reason) => {
  console.error('[FATAL] unhandledRejection:', reason);
});

// ---------------------------------------------------------------------
// Arquivos CSV esperados no diretório do app
// ---------------------------------------------------------------------
const CSV_SEP = ';';

const PRODUCTS_FILE = path.join(__dirname, 'produtos.csv');
const PDVS_FILE     = path.join(__dirname, 'pdvs_final.csv');
const PDV_PROD_FILE = path.join(__dirname, 'pdv_produtos_filtrado_final.csv');

// Checagem inicial — ajuda a detectar “ENOENT” logo de cara
function statOrNull(p) {
  try { return fs.statSync(p); } catch { return null; }
}
console.log('[BOOT] __dirname =', __dirname);
console.log('[BOOT] PRODUCTS_FILE =', PRODUCTS_FILE, statOrNull(PRODUCTS_FILE) ? 'OK' : 'NOT FOUND');
console.log('[BOOT] PDVS_FILE     =', PDVS_FILE,     statOrNull(PDVS_FILE)     ? 'OK' : 'NOT FOUND');
console.log('[BOOT] PDV_PROD_FILE =', PDV_PROD_FILE, statOrNull(PDV_PROD_FILE) ? 'OK' : 'NOT FOUND');

// ---------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------
function normalizeRow(row) {
  const out = {};
  for (const [k, v] of Object.entries(row)) {
    let key = String(k).trim().toLowerCase();
    key = key.replace(/^\uFEFF/, ''); // remove BOM
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

// ---------------------------------------------------------------------
// Leitura dos CSVs
// ---------------------------------------------------------------------
function loadProductsCsv() {
  return new Promise((resolve, reject) => {
    const items = [];
    fs.createReadStream(PRODUCTS_FILE)
      .on('error', (e) => {
        console.error('[CSV produtos] ERRO ao abrir arquivo:', e.message);
        reject(e);
      })
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
      .on('end', () => {
        console.log(`[CSV produtos] lidos ${items.length} registros`);
        if (items.length) console.log('[CSV produtos] 1º registro:', items[0]);
        resolve(items);
      })
      .on('error', (e) => {
        console.error('[CSV produtos] ERRO no parser:', e);
        reject(e);
      });
  });
}

function loadPdvsMap() {
  return new Promise((resolve, reject) => {
    const map = new Map();
    fs.createReadStream(PDVS_FILE)
      .on('error', (e) => {
        console.error('[CSV pdvs] ERRO ao abrir arquivo:', e.message);
        reject(e);
      })
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
      .on('end', () => {
        console.log(`[CSV pdvs] mapeados ${map.size} PDVs`);
        resolve(map);
      })
      .on('error', (e) => {
        console.error('[CSV pdvs] ERRO no parser:', e);
        reject(e);
      });
  });
}

// ---------------------------------------------------------------------
// Rotas
// ---------------------------------------------------------------------
app.get('/health', (_, res) => res.json({ ok: true }));

app.get('/', (_, res) => {
  res.type('text/plain').send('OK - backend online');
});

// Produtos em destaque
app.get('/produtos/destaque', async (req, res) => {
  try {
    const all = await loadProductsCsv();
    const destacados = all.filter(p => p.em_destaque);
    return res.json(destacados); // ATENÇÃO: variável correta
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

// PDVs próximos por CEP (permanece igual)
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
      .pipe(csv({ separator: CSV_SEP, mapHeaders: ({ header }) => header.trim() }))
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
    console.error('Erro CEP:', e);
    res.status(500).json({ erro: 'Erro ao buscar coordenadas do CEP.' });
  }
});

// Função distância (usada acima)
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
app.listen(PORT, () => {
  console.log(`Servidor rodando na porta ${PORT}`);
});
