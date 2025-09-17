const path = require('path');
const fs = require('fs');

// Caminhos dos CSVs (MESMA PASTA do server.js)
const storesCsvPath      = path.join(__dirname, 'pdvs_final.csv');
const productsCsvPath    = path.join(__dirname, 'produtos.csv');
const pdvProductsCsvPath = path.join(__dirname, 'pdv_produtos_filtrado_final.csv');

// (Opcional mas útil) Logar se o arquivo existe, pra ver nos logs do Render:
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
ensureFileOrWarn(storesCsvPath, 'pontos_de_venda_final.csv');
