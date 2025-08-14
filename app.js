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

// ROTA: Produtos em Destaque
app.get('/produtos/destaque', (req, res) => {
  const results = [];
  fs.createReadStream(path.join(__dirname, 'produtos.csv'))
    .pipe(csv())
    .on('data', (data) => {
      if (data.em_destaque === 'TRUE') results.push(data);
    })
    .on('end', () => res.json(results));
});

// ROTA: Buscar produtos por nome
app.get('/produtos/buscar', (req, res) => {
  const searchTerm = req.query.q;
  if (!searchTerm) return res.status(400).json({ erro: 'Termo de busca é obrigatório.' });

  const results = [];
  fs.createReadStream(path.join(__dirname, 'produtos.csv'))
    .pipe(csv())
    .on('data', (data) => {
      if (data.nome.toLowerCase().includes(searchTerm.toLowerCase())) {
        results.push(data);
      }
    })
    .on('end', () => res.json(results));
});

// ROTA: Buscar PDVs por CEP
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
      .pipe(csv())
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
      });
  } catch (error) {
    res.status(500).json({ erro: 'Erro ao buscar coordenadas do CEP.' });
  }
});

// ROTA: Buscar PDVs por coordenadas
app.get('/pdvs/proximos/coords', (req, res) => {
  const userLat = parseFloat(req.query.lat);
  const userLon = parseFloat(req.query.lon);
  if (isNaN(userLat) || isNaN(userLon)) return res.status(400).json({ erro: 'Coordenadas inválidas.' });

  const pdvs = [];
  fs.createReadStream(path.join(__dirname, 'pontos_de_venda_final.csv'))
    .pipe(csv())
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
    });
});

// ROTA: Buscar PDVs por produto e localização
app.get('/pdvs/proximos/produto', (req, res) => {
  const { productId, lat, lon } = req.query;
  const userLat = parseFloat(lat);
  const userLon = parseFloat(lon);
  if (!productId || isNaN(userLat) || isNaN(userLon)) {
    return res.status(400).json({ erro: 'Parâmetros inválidos.' });
  }

  const pdvs = [];
  fs.createReadStream(path.join(__dirname, 'pdv_produtos_filtrado_final.csv'))
    .pipe(csv())
    .on('data', (row) => {
      if (row.produto_id === productId) {
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
    });
});

app.listen(PORT, () => {
  console.log(`Servidor rodando na porta ${PORT}`);
});
