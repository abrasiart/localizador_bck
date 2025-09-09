const path = require('path');
const fs = require('fs');
const csv = require('csv-parser');
const fetch = require('node-fetch');

const CSV_SEP = ';';
const PDVS_FILE = path.join(__dirname, 'pdvs_final.csv');
const GEOCODE_CACHE_FILE = path.join(__dirname, 'geocode_cache.json');
const OPENCAGE_KEY = process.env.OPENCAGE_KEY || '';

let cache = Object.create(null);
try {
  if (fs.existsSync(GEOCODE_CACHE_FILE)) {
    cache = JSON.parse(fs.readFileSync(GEOCODE_CACHE_FILE,'utf-8'));
  }
} catch {}

function norm(s){ return String(s??'').trim(); }
function onlyDigits(s, max){ const d = String(s??'').replace(/\D/g,''); return max? d.slice(0,max) : d; }
function buildAddress(row){
  const rua = norm(row.rua), bairro=norm(row.bairro), cidade=norm(row.cidade), uf=norm(row.estado), cep=onlyDigits(row.cep,8);
  const parts=[];
  if(rua) parts.push(rua); if(bairro) parts.push(bairro); if(cidade) parts.push(cidade);
  if(uf) parts.push(`${uf}, Brasil`); else parts.push('Brasil');
  if(cep) parts.push(cep);
  return parts.join(', ');
}

async function geocode(address){
  if (!OPENCAGE_KEY) return null;
  if (cache[address]) return cache[address];
  const url = `https://api.opencagedata.com/geocode/v1/json?q=${encodeURIComponent(address)}&key=${OPENCAGE_KEY}&limit=1&no_annotations=1&language=pt-BR`;
  const r = await fetch(url);
  if (!r.ok) return null;
  const j = await r.json();
  const first = j?.results?.[0];
  if (!first?.geometry) return null;
  cache[address] = { lat:+first.geometry.lat, lon:+first.geometry.lng };
  return cache[address];
}

(async () => {
  if (!OPENCAGE_KEY) {
    console.log('OPENCAGE_KEY não definido — encerrando.');
    process.exit(0);
  }
  const addrs = [];
  await new Promise((resolve,reject)=>{
    fs.createReadStream(PDVS_FILE)
      .pipe(csv({ separator: CSV_SEP, mapHeaders: ({header}) => header.trim().toLowerCase() }))
      .on('data',(row)=> addrs.push(buildAddress(row)))
      .on('end', resolve)
      .on('error', reject);
  });

  console.log(`Geocodificando ${addrs.length} endereços...`);
  // limite de concorrência simples
  const queue = [...new Set(addrs)]; // sem duplicadas
  const CONC = 3;
  let running = 0, i = 0;

  await new Promise((resolve) => {
    const tick = () => {
      if (i >= queue.length && running === 0) return resolve();
      while (running < CONC && i < queue.length) {
        const addr = queue[i++]; running++;
        geocode(addr).finally(()=>{ running--; setTimeout(tick, 150); });
      }
    };
    tick();
  });

  fs.writeFileSync(GEOCODE_CACHE_FILE, JSON.stringify(cache,null,2), 'utf-8');
  console.log('Cache salvo em geocode_cache.json');
})();
