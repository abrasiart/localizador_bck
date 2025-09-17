// server.js (CommonJS)

const express = require('express');
const cors = require('cors');
const fs = require('fs');
const csv = require('csv-parser');
const path = require('path');

// ========= CONFIG BÁSICA =========
const app = express();
const PORT = process.env.PORT || 4000;       // Render usa process.env.PORT
app.use(express.json());

// Ajuste o domínio do Vercel quando tiver:
app.use(cors({
  origin: [
    'http://localhost:3000',
    'https://SEU-PROJETO.vercel.app'
  ],
  methods: ['GET','POST','PUT','DELETE','OPTIONS'],
  allowedHeaders: ['Content-Type','Authorization']
}));
app.options('*', cors());

// ========= ESTADO EM MEMÓRIA =========
/** @type {Array<{id:string,nome:string,volume:string,em_destaque:boolean,imagem_url:string}>} */
let products = [];
/** @type {Array<any>} */
let stores = [];
/** Map de PDV -> [productId, ...] */
let pdvProductsMapping = Object.create(null);
/** Map rápido de produto por id */
let productById = Object.create(null);

// ========= ARQUIVOS CSV =========
// Coloque os CSVs na MESMA pasta do server.js ou ajuste os caminhos:
const storesCsvPath = path.join(__dirname, 'pontos_de_venda_final.csv');
const productsCsvPath = path.join(__dirname, 'produtos.csv');
const pdvProductsCsvPath = path.join(__dirname, 'pdv_produtos_filtrado_final.csv');

// ========= HELPERS =========
function loadCsv(filePath, onRow) {
  return new Promise((resolve, reject) => {
    fs.createReadStream(filePath)
      .pipe(csv({ separator: ';' }))
      .on('data', (row) => {
        try { onRow(row); } catch (e) { console.error('Erro onRow:', e); }
      })
      .on('end', resolve)
      .on('error', reject);
  });
}

async function loadProducts() {
  products = [];
  await loadCsv(productsCsvPath, (data) => {
    const p = {
      id: String(data.id ?? '').trim(),
      nome: String(data.nome ?? '').trim(),
      volume: String(data.volume ?? '').trim(),
      em_destaque: String(data.em_destaque ?? '').trim().toUpperCase() === 'TRUE',
      imagem_url: String(data.imagem_url ?? '').trim()
    };
    if (p.id) {
      products.push(p);
    }
  });
  // índice por id
  productById = Object.create(null);
  for (const p of products) productById[p.id] = p;
  console.log(`Produtos carregados: ${products.length}`);
}

async function loadPdvProductsMapping() {
  pdvProductsMapping = Object.create(null);
  await loadCsv(pdvProductsCsvPath, (data) => {
    const id_pdv = String(data.id_pdv ?? '').trim();
    const codigo = String(data.codigo ?? '').trim();   // ID do produto
    if (!id_pdv || !codigo) return;
    if (!pdvProductsMapping[id_pdv]) pdvProductsMapping[id_pdv] = [];
    pdvProductsMapping[id_pdv].push(codigo);
  });
  console.log('Mapeamento PDV-Produtos carregado.');
}

async function loadStores() {
  stores = [];
  await loadCsv(storesCsvPath, (data) => {
    // normaliza e converte campos úteis
    const s = {
      ...data,
      id: String(data.id ?? '').trim(),
      nome: String(data.nome ?? data.loja ?? '').trim(),
      cep: String(data.cep ?? '').replace(/\D/g, ''),
      endereco: String(data.endereco ?? data.endereço ?? '').trim(),
      latitude: Number(String(data.latitude ?? '').replace(',', '.')),
      longitude: Number(String(data.longitude ?? '').replace(',', '.'))
    };
    if (!s.id) return;

    // anexa os productIds a partir do mapping
    const productIds = pdvProductsMapping[s.id] || [];
    s.products = productIds; // guardamos IDs (não nomes)

    stores.push(s);
  });
  console.log(`PDVs carregados: ${stores.length}`);
}

// Distância Haversine (km)
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

// ========= CARGA INICIAL =========
async function bootstrap() {
  try {
    await loadProducts();
    await loadPdvProductsMapping();
    await loadStores();
    console.log('Carga completa.');
  } catch (e) {
    console.error('Falha ao carregar CSVs:', e);
  }
}
bootstrap();

// ========= ROTAS =========
app.get('/health', (req, res) => res.json({ ok: true }));

// --- SUAS ROTAS ANTIGAS (mantidas) ---

// Lista completa de produtos
app.get('/products', (req, res) => {
  return res.json(products);
});

// PDVs por produtoId (filtra por ID, pois stores.products guarda IDs)
app.get('/stores-by-product', (req, res) => {
  const productId = String(req.query.productId ?? '').trim();
  if (!productId) {
    return res.status(400).json({ erro: 'O parâmetro productId é obrigatório.' });
  }
  const filtered = stores.filter(s => Array.isArray(s.products) && s.products.includes(productId));
  return res.json(filtered);
});

// --- NOVAS ROTAS COMPATÍVEIS COM O OUTRO FRONT ---

// 1) Produtos em destaque
app.get('/produtos/destaque', (req, res) => {
  const destaque = products.filter(p => p.em_destaque);
  return res.json(destaque);
});

// 2) Buscar produtos por termo ?q=
app.get('/produtos/buscar', (req, res) => {
  const q = String(req.query.q ?? '').trim().toLowerCase();
  if (!q) return res.json([]);
  const result = products.filter(p =>
    p.nome.toLowerCase().includes(q) || p.volume.toLowerCase().includes(q)
  );
  return res.json(result);
});

// 3) PDVs próximos por CEP (stub simples: filtra por campo cep se existir; senão retorna alguns)
app.get('/pdvs/proximos', (req, res) => {
  const cep = String(req.query.cep ?? '').replace(/\D/g, '');
  if (!cep || cep.length !== 8) {
    return res.status(400).json({ erro: 'CEP inválido. Use 8 dígitos.' });
  }
  // se CSV tem cep, filtra pelos que batem; senão devolve primeiros como fallback
  const byCep = stores.filter(s => s.cep && s.cep.startsWith(cep.substring(0,5)));
  return res.json(byCep.length ? byCep : stores.slice(0, 5));
});

// 4) PDVs próximos por produto + coords
// GET /pdvs/proximos/produto?productId=ID&lat=-26.30&lon=-48.84
app.get('/pdvs/proximos/produto', (req, res) => {
  const productId = String(req.query.productId ?? '').trim();
  const lat = Number(req.query.lat);
  const lon = Number(req.query.lon);

  if (!productId || Number.isNaN(lat) || Number.isNaN(lon)) {
    return res.status(400).json({ erro: 'Parâmetros obrigatórios: productId, lat, lon.' });
  }

  // PDVs que vendem o produto
  const candidates = stores.filter(s => Array.isArray(s.products) && s.products.includes(productId));

  // calcula distância e ordena
  const withDist = candidates.map(s => {
    const distancia_km = (typeof s.latitude === 'number' && typeof s.longitude === 'number')
      ? haversineKm(lat, lon, s.latitude, s.longitude)
      : null;
    return { ...s, distancia_km };
  }).sort((a, b) => {
    if (a.distancia_km == null) return 1;
    if (b.distancia_km == null) return -1;
    return a.distancia_km - b.distancia_km;
  });

  // limita a 20 resultados
  return res.json(withDist.slice(0, 20));
});

// ========= START =========
app.listen(PORT, () => {
  console.log(`API escutando na porta ${PORT}`);
});
