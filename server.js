import express from "express";
import cors from "cors";

const express = require('express');
const cors = require('cors');
const fs = require('fs');
const csv = require('csv-parser');
const path = require('path');

const app = express();
const port = 5000;

app.use(cors());
app.use(express.json());

let stores = [];
let products = [];
let pdvProductsMapping = {};

const storesCsvPath = path.join(__dirname, 'pontos_de_venda_final.csv');
const productsCsvPath = path.join(__dirname, 'produtos.csv');
const pdvProductsCsvPath = path.join(__dirname, 'pdv_produtos_filtrado_final.csv');

// Função para carregar dados do CSV (assumindo ponto e vírgula como delimitador)
const loadCsv = (filePath, onData, onEnd) => {
  fs.createReadStream(filePath)
    .pipe(csv({ separator: ';' }))
    .on('data', (data) => onData(data))
    .on('end', () => onEnd())
    .on('error', (err) => console.error(`Erro ao ler o arquivo ${filePath}:`, err));
};

// 1. Carrega a lista de produtos
const loadProducts = () => {
  loadCsv(
    productsCsvPath,
    (data) => {
      products.push({
        id: data.id,
        nome: data.nome,
        volume: data.volume,
        em_destaque: data.em_destaque === 'TRUE',
        imagem_url: data.imagem_url,
      });
    },
    () => console.log(`Dados de ${products.length} produtos carregados.`)
  );
};

// 2. Carrega o mapeamento PDV -> Produtos
const loadPdvProductsMapping = () => {
  loadCsv(
    pdvProductsCsvPath,
    (data) => {
      if (!pdvProductsMapping[data.id_pdv]) {
        pdvProductsMapping[data.id_pdv] = [];
      }
      pdvProductsMapping[data.id_pdv].push(data.codigo);
    },
    () => console.log('Mapeamento PDV-Produtos carregado.')
  );
};

// 3. Carrega a lista de PDVs e adiciona os produtos mapeados
const loadStores = () => {
  loadCsv(
    storesCsvPath,
    (data) => {
      data.latitude = parseFloat(data.latitude);
      data.longitude = parseFloat(data.longitude);
      const availableProductCodes = pdvProductsMapping[data.id] || [];
      data.products = availableProductCodes.map(code => {
        const product = products.find(p => p.id === code);
        return product ? product.nome : null;
      }).filter(name => name);
      stores.push(data);
    },
    ()rag() => console.log(`Dados de ${stores.length} PDVs carregados.`)
  );
};

// Sequência de carregamento
loadProducts();
setTimeout(() => {
    loadPdvProductsMapping();
    setTimeout(loadStores, 500);
}, 500);

// Rota para buscar a lista de produtos
app.get('/products', (req, res) => {
  return res.json(products);
});

// Rota para buscar PDVs por produto
app.get('/stores-by-product', (req, res) => {
    const { productId } = req.query;
    if (!productId) {
        return res.status(400).json({ erro: 'O parâmetro productId é obrigatório.' });
    }

    const filteredStores = stores.filter(store => store.products.includes(productId));
    return res.json(filteredStores);
});

app.listen(port, () => {
  console.log(`Backend rodando em http://localhost:${port}`);
});