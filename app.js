const express = require('express');
const cors = require('cors');
const path = require('path');
const fetch = require('node-fetch').default; // Acesse a exportação 'default' do node-fetch
const { Pool } = require('pg');

const app = express();
const PORT = 5000;

const pool = new Pool({
    user: 'postgres',
    host: 'localhost',
    database: 'paviloche_localizador_db',
    password: '4971', // <<<< ATENÇÃO: COLOQUE A SENHA CORRETA AQUI!
    port: 5432,
});

pool.on('connect', () => {
    console.log('Conectado ao PostgreSQL!');
});
pool.on('error', (err) => {
    console.error('Erro de conexão com o PostgreSQL:', err);
});

app.use(cors());
app.use(express.json());

function calculateDistance(lat1, lon1, lat2, lon2) {
    const R = 6371; // Raio da Terra em quilômetros
    const dLat = (lat2 - lat1) * Math.PI / 180;
    const dLon = (lon2 - lon1) * Math.PI / 180;
    const a =
        Math.sin(dLat / 2) * Math.sin(dLat / 2) +
        Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
        Math.sin(dLon / 2) * Math.sin(dLon / 2);
    const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    const distance = R * c;
    return distance;
}

// ROTA: Produtos em Destaque
app.get('/produtos/destaque', async (req, res) => {
    try {
        const result = await pool.query('SELECT * FROM produtos WHERE em_destaque = TRUE');
        res.json(result.rows);
    } catch (error) {
        console.error('Erro ao buscar produtos em destaque do DB:', error.message); // Removido ': any'
        res.status(500).json({ erro: 'Erro interno ao buscar produtos em destaque.' });
    }
});


// Rota para buscar PDVs por CEP
app.get('/pdvs/proximos', async (req, res) => {
    const userCep = req.query.cep;

    console.log(`[Backend Debug - /pdvs/proximos] Requisição recebida para CEP: ${userCep}`);

    if (!userCep) {
        console.warn('[Backend Debug - /pdvs/proximos] Erro 400: CEP é obrigatório.');
        return res.status(400).json({ erro: 'CEP é obrigatório.' });
    }

    const cleanCep = String(userCep).replace(/\D/g, '');
    console.log(`[Backend Debug - /pdvs/proximos] CEP limpo: ${cleanCep}`);

    if (cleanCep.length !== 8) {
        console.warn(`[Backend Debug - /pdvs/proximos] Erro 400: CEP inválido (${cleanCep}).`);
        return res.status(400).json({ erro: 'CEP inválido. Deve conter 8 dígitos.' });
    }

    try {
        console.log(`[Backend Debug - /pdvs/proximos] Chamando AwesomeAPI para CEP: ${cleanCep}`);
        const awesomeApiUrl = `https://cep.awesomeapi.com.br/json/${cleanCep}`;
        const awesomeApiResponse = await fetch(awesomeApiUrl);
        const awesomeApiData = await awesomeApiResponse.json();

        console.log(`[Backend Debug - /pdvs/proximos] Resposta da AwesomeAPI:`, awesomeApiData);

        if (awesomeApiData.code === 'not_found' || !awesomeApiData.lat || !awesomeApiData.lng) {
            console.warn(`[Backend Debug - /pdvs/proximos] Erro 404: CEP ${cleanCep} não encontrado pela AwesomeAPI.`);
            return res.status(404).json({ erro: 'CEP não encontrado ou inválido pela AwesomeAPI. Por favor, verifique o CEP digitado.' });
        }

        const userLat = parseFloat(awesomeApiData.lat);
        const userLon = parseFloat(awesomeApiData.lng);
        console.log(`[Backend Debug - /pdvs/proximos] Coordenadas do usuário: Lat ${userLat}, Lon ${userLon}`);

        if (isNaN(userLat) || isNaN(userLon)) {
            console.error(`[Backend Debug - /pdvs/proximos] Erro 500: Coordenadas do CEP inválidas após parseFloat (${awesomeApiData.lat}, ${awesomeApiData.lng}).`);
            return res.status(500).json({ erro: 'Erro ao processar coordenadas do CEP.' });
        }

        console.log('[Backend Debug - /pdvs/proximos] Buscando PDVs no DB...');
        const result = await pool.query('SELECT * FROM pontos_de_venda WHERE latitude IS NOT NULL AND longitude IS NOT NULL');
        const pdvsFromDb = result.rows;
        console.log(`[Backend Debug - /pdvs/proximos] Encontrados ${pdvsFromDb.length} PDVs no DB para cálculo.`);

        const pdvsWithDistance = pdvsFromDb.map(pdv => {
            const pdvLat = parseFloat(pdv.latitude);
            const pdvLon = parseFloat(pdv.longitude);

            if (isNaN(pdvLat) || isNaN(pdvLon)) {
                console.warn(`[Backend Debug - /pdvs/proximos] Aviso: PDV ${pdv.id} tem coordenadas inválidas no DB (${pdv.latitude}, ${pdv.longitude}). Será ignorado.`);
                return null;
            }

            const distance = calculateDistance(userLat, userLon, pdvLat, pdvLon);
            return {
                ...pdv,
                distancia_km: parseFloat(distance.toFixed(2))
            };
        }).filter(pdv => pdv !== null)
        .sort((a, b) => a.distancia_km - b.distancia_km);

        console.log(`[Backend Debug - /pdvs/proximos] Retornando ${pdvsWithDistance.length} PDVs com distância calculada.`);
        res.json(pdvsWithDistance);

    } catch (error) {
        console.error('[Backend Debug - /pdvs/proximos] ERRO FATAL na rota /pdvs/proximos:', error);
        console.error('[Backend Debug - /pdvs/proximos] Mensagem do erro:', error.message);
        console.error('[Backend Debug - /pdvs/proximos] Stack trace:', error.stack);
        res.status(500).json({ erro: 'Erro interno do servidor ao processar sua requisição por CEP.' });
    }
});

// Rota para buscar PDVs por Latitude e Longitude
app.get('/pdvs/proximos/coords', async (req, res) => {
    const userLatStr = req.query.lat;
    const userLonStr = req.query.lon;

    if (!userLatStr || !userLonStr) {
        return res.status(400).json({ erro: 'Latitude e Longitude são obrigatórias.' });
    }
    const userLat = parseFloat(userLatStr);
    const userLon = parseFloat(userLonStr);
    if (isNaN(userLat) || isNaN(userLon)) {
        return res.status(400).json({ erro: 'Latitude ou Longitude inválidas.' });
    }

    try {
        const result = await pool.query('SELECT * FROM pontos_de_venda WHERE latitude IS NOT NULL AND longitude IS NOT NULL');
        const pdvsFromDb = result.rows;

        const pdvsWithDistance = pdvsFromDb.map(pdv => {
            const distance = calculateDistance(userLat, userLon, parseFloat(pdv.latitude), parseFloat(pdv.longitude));
            return {
                ...pdv,
                distancia_km: parseFloat(distance.toFixed(2))
            };
        }).sort((a, b) => a.distancia_km - b.distancia_km);

        res.json(pdvsWithDistance);

    } catch (error) {
        console.error('Erro ao buscar PDVs por coordenadas (DB):', error);
        res.status(500).json({ erro: 'Erro interno do servidor ao processar sua requisição por coordenadas.' });
    }
});

// Rota para buscar produtos por nome
app.get('/produtos/buscar', async (req, res) => {
    const searchTerm = req.query.q;

    if (!searchTerm) {
        return res.status(400).json({ erro: 'Termo de busca é obrigatório.' });
    }
    const lowerCaseSearchTerm = String(searchTerm).toLowerCase();

    try {
        const queryText = 'SELECT * FROM produtos WHERE LOWER(nome) LIKE $1';
        const queryParams = [`%${lowerCaseSearchTerm}%`];
        const result = await pool.query(queryText, queryParams);

        res.json(result.rows);
    } catch (error) {
        console.error('Erro ao buscar produtos por nome do DB:', error);
        res.status(500).json({ erro: 'Erro interno do servidor ao buscar produtos.' });
    }
});

// NOVA ROTA: Buscar PDVs por Produto e Localização
app.get('/pdvs/proximos/produto', async (req, res) => {
    const { productId, lat, lon } = req.query;

    console.log(`[Backend Debug] Requisição para /pdvs/proximos/produto:`);
    console.log(`  - productId: ${productId}`);
    console.log(`  - lat: ${lat}`);
    console.log(`  - lon: ${lon}`);

    if (!productId || !lat || !lon) {
        console.warn('[Backend Debug] Erro 400: Parâmetros faltando.');
        return res.status(400).json({ erro: 'ID do produto, Latitude e Longitude são obrigatórios.' });
    }

    const userLat = parseFloat(lat);
    const userLon = parseFloat(lon);

    if (isNaN(userLat) || isNaN(userLon)) {
        console.warn('[Backend Debug] Erro 400: Latitude ou Longitude inválidas.');
        return res.status(400).json({ erro: 'Latitude ou Longitude inválidas.' });
    }

    try {
        console.log(`[Backend Debug] Buscando PDVs no DB para productId: ${productId}`);
        const queryText = `
            SELECT
                p.*
            FROM
                pontos_de_venda p
            JOIN
                pdv_produtos pp ON p.id = pp.pdv_id
            WHERE
                pp.produto_id = $1 AND
                p.latitude IS NOT NULL AND p.longitude IS NOT NULL;
        `;
        const queryParams = [productId];

        const result = await pool.query(queryText, queryParams);
        const pdvsFromDb = result.rows;

        console.log(`[Backend Debug] Encontrados ${pdvsFromDb.length} PDVs no DB antes do cálculo de distância.`);

        const pdvsWithDistance = pdvsFromDb.map(pdv => {
            const pdvLat = parseFloat(pdv.latitude);
            const pdvLon = parseFloat(pdv.longitude);

            if (isNaN(pdvLat) || isNaN(pdvLon)) {
                console.warn(`[Backend Debug] Aviso: PDV ${pdv.id} tem coordenadas inválidas no DB. Pulando cálculo.`);
                return null;
            }

            const distance = calculateDistance(userLat, userLon, pdvLat, pdvLon);
            return {
                ...pdv,
                distancia_km: parseFloat(distance.toFixed(2))
            };
        }).filter(pdv => pdv !== null)
        .sort((a, b) => a.distancia_km - b.distancia_km);

        console.log(`[Backend Debug] Retornando ${pdvsWithDistance.length} PDVs com distância calculada.`);
        res.json(pdvsWithDistance);

    } catch (error) {
        console.error('[Backend Debug] ERRO DETALHADO na rota /pdvs/proximos/produto:', error);
        console.error('[Backend Debug] Mensagem do erro:', error.message);
        console.error('[Backend Debug] Stack trace:', error.stack);
        res.status(500).json({ erro: 'Erro interno do servidor ao buscar PDVs por produto.' });
    }
});


app.listen(PORT, () => {
    console.log(`Backend rodando na porta ${PORT}`);
    console.log(`Acesse http://localhost:${PORT}`);
});