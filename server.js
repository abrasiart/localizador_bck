// server.js (CommonJS)

const express = require('express');
const cors = require('cors');
const fs = require('fs');
const csv = require('csv-parser');
const path = require('path');

// ================== CONFIG BÁSICA ==================
const app = express();
const PORT = process.env.PORT || 4000;

app.use(express.json());
app.use(cors({
  origin: [
    'http://localhost:3000',
    'http://localhost:5173',
    'https://SEU-PROJETO.vercel.app' // troque pelo seu domínio do Vercel
  ],
  methods: ['GET','POST','PUT','DELETE','OPTIONS'],
  allowedHeaders: ['Content-Type','Authorization']
}));
app.options('*', cors());

// ================== ARQUIVOS CSV ==================
// MESMO diretório do server.js — nomes conforme você enviou
const productsCsvPath    = path.join(__dirname, 'produtos.csv');
const pdvProductsCsvPath = path.join(__dirname, 'pdv_produtos_filtrado_final.csv');
const storesCsvPath      = path.join(__dirname, 'pdvs_final.csv');

// (Logs úteis para ver no Render)
function ensureFileOrWarn(filePath, name) {
  if (!fs.existsSync(filePath)) {
    console.warn(`[WARN] Arquivo ausente: ${name} -> ${filePath}`);
    return false;
  }
  console.log(`[OK] Arquivo encontrado: ${name} -> ${filePath}`);
  return true;
}
ensureFileOrWarn(productsCsvPath, 'produtos.csv');
ensureFileOrWarn(pdvProductsCsvPath, 'pdv_produtos_filtrado_final.csv');
ensureFileOrWarn(storesCsvPath, 'pdvs_final.csv');

// ================== ESTADO EM MEMÓRIA ==================
/** @type {Array<{id:string,nome:string,volume:string,em_destaque:boolean,imagem_url:string}>} */
let products = [];
/** @type {Array<any>} */
let stores = [];
/** Map de PDV -> [productId,...] */
let pdvProductsMapping = Object.create(null);
/** Índice rápido por id de produto */
let productById = Object.create(null);

// ================== HELPERS ==================
function loadCsv(filePath, onRow) {
  return new Promise((resolve, reject) => {
    if (!fs.existsSync(filePath)) {
      console.warn(`[WARN] Ignorando carga: arquivo não existe -> ${filePath}`);
      return resolve();
    }
    fs.createReadStream(filePath)
      .pipe(csv({ separator: ';' }))
      .on('data', row => {
        try { onRow(row); } catch (e) { console.error('Erro onRow:', e); }
      })
      .on('end', resolve)
      .on('error', reject);
  });
}

async function loadProducts() {
  try {
    products = [];
    await loadCsv(productsCsvPath, (data) => {
      const p = {
        id: String(data.id ?? '').trim(),
        nome: String(data.nome ?? '').trim(),
        volume: String(data.volume ?? '').trim(),
        em_destaque: String(data.em_destaque ?? '').trim().toUpperCase() === 'TRUE',
        imagem_url: String(data.imagem_url ?? '').trim()
      };
      if (p.id) products.push(p);
    });
    productById = Object.create(null);
    for (const p of products) productById[p.id] = p;
    console.log(`Produtos carregados: ${products.length}`);
  } catch (e) {
    console.error('Falha ao carregar produtos.csv:', e);
  }
}

async function loadPdvProductsMapping() {
  try {
    pdvProductsMapping = Object.create(null);
    await loadCsv(pdvProductsCsvPath, (data) => {
      const id_pdv = String(data.id_pdv ?? '').trim();
      const codigo = String(data.codigo ?? '').trim();  // id do produto
      if (!id_pdv || !codigo) return;
      if (!pdvProductsMapping[id_pdv]) pdvProductsMapping[id_pdv] = [];
      pdvProductsMapping[id_pdv].push(codigo);
    });
    console.log('Mapeamento PDV-Produtos carregado.');
  } catch (e) {
    console.error('Falha ao carregar pdv_produtos_filtrado_final.csv:', e);
  }
}

async function loadStores() {
  try {
    stores = [];
    await loadCsv(storesCsvPath, (data) => {
      const s = {
        ...data,
        id: String(data.id ?? '').trim(),
        nome: String(data.nome ?? data.loja ?? '').trim(),
        cep: String(data.cep ?? '').replace(/\D/g, ''),
        endereco: String(data.endereco ?? data.endereço ?? '').trim(),
        latitude: toNum(data.latitude),
        longitude: toNum(data.longitude)
      };
      if (!s.id) return;
      s.products = pdvProductsMapping[s.id] || []; // guardamos IDs de produto
      stores.push(s);
    });
    console.log(`PDVs carregados: ${stores.length}`);
  } catch (e) {
    console.error('Falha ao carregar pdvs_final.csv:', e);
  }
}

function toNum(v) {
  if (v == null) return NaN;
  const n = Number(String(v).replace(',', '.'));
  return Number.isFinite(n) ? n : NaN;
}

function haversineKm(lat1, lon1, lat2, lon2) {
  const toRad = (d) => (d * Math.PI) / 180;
  const R = 6371;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const a =
    Math.sin(dLat/2)**2 +
    Math.cos(toRad(lat1))*Math.cos(toRad(lat2))*Math.sin(dLon/2)**2;
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
  return +(R * c).toFixed(2);
}

async function bootstrap() {
  // carrega em sequência; se algum falhar, segue com o que der
  await loadProducts();
  await loadPdvProductsMapping();
  await loadStores();
  console.log('Carga completa.', { products: products.length, stores: stores.length });
}

// ================== ROTAS ==================
app.get('/health', (req, res) => {
  res.json({
    ok: true,
    counts: { products: products.length, stores: stores.length }
  });
});

// Produtos em destaque
app.get('/produtos/destaque', (req, res) => {
  return res.json(products.filter(p => p.em_destaque));
});

// Buscar produtos por termo ?q=
app.get('/produtos/buscar', (req, res) => {
  const q = String(req.query.q ?? '').trim().toLowerCase();
  if (!q) return res.json([]);
  const result = products.filter(p =>
    p.nome.toLowerCase().includes(q) || p.volume.toLowerCase().includes(q)
  );
  return res.json(result);
});

// PDVs por produto (id)
app.get('/stores-by-product', (req, res) => {
  const productId = String(req.query.productId ?? '').trim();
  if (!productId) {
    return res.status(400).json({ erro: 'O parâmetro productId é obrigatório.' });
  }
  const filtered = stores.filter(s => Array.isArray(s.products) && s.products.includes(productId));
  return res.json(filtered);
});

// PDVs próximos por CEP (stub simples por prefixo)
app.get('/pdvs/proximos', (req, res) => {
  const cep = String(req.query.cep ?? '').replace(/\D/g, '');
  if (!cep || cep.length !== 8) {
    return res.status(400).json({ erro: 'CEP inválido. Use 8 dígitos.' });
  }
  const byCep = stores.filter(s => s.cep && s.cep.startsWith(cep.slice(0,5)));
  return res.json(byCep.length ? byCep : stores.slice(0, 5));
});

// PDVs próximos por produto + coords
// GET /pdvs/proximos/produto?productId=ID&lat=-26.30&lon=-48.84
app.get('/pdvs/proximos/produto', (req, res) => {
  const productId = String(req.query.productId ?? '').trim();
  const lat = Number(req.query.lat);
  const lon = Number(req.query.lon);

  if (!productId || Number.isNaN(lat) || Number.isNaN(lon)) {
    return res.status(400).json({ erro: 'Parâmetros obrigatórios: productId, lat, lon.' });
    }

  const candidates = stores.filter(s => Array.isArray(s.products) && s.products.includes(productId));
  const withDist = candidates.map(s => {
    const distancia_km = (Number.isFinite(s.latitude) && Number.isFinite(s.longitude))
      ? haversineKm(lat, lon, s.latitude, s.longitude)
      : null;
    return { ...s, distancia_km };
  }).sort((a, b) => {
    if (a.distancia_km == null) return 1;
    if (b.distancia_km == null) return -1;
    return a.distancia_km - b.distancia_km;
  });

  return res.json(withDist.slice(0, 20));
});

// ================== START: ABRE A PORTA JÁ ==================
app.listen(PORT, () => {
  console.log(`API escutando na porta ${PORT}`);
  bootstrap().catch(e => console.error('Falha no bootstrap:', e));
});

// Evita encerramento por erros não tratados
process.on('unhandledRejection', (r) => console.error('unhandledRejection:', r));
process.on('uncaughtException', (e) => console.error('uncaughtException:', e));
