# backend/geocodificar_pdvs.py (Geocodificação inteligente: CEP com AwesomeAPI, Endereço com OpenCage)

import pandas as pd
import requests
import time
from urllib.parse import urlencode 

# --- Configurações ---
INPUT_CSV_FILE = 'pdvs_2.csv'  # Nome do seu arquivo CSV de entrada
OUTPUT_CSV_FILE = 'pdvs_2_com_coords.csv' # Nome do novo arquivo CSV de saída

# SUAS CHAVES DE API
# Chave OpenCage (para geocodificação de endereço, fallback)
OPENCAGE_API_KEY = '0b4186d795a547769c0272db912585c3' # <-- SEU API KEY DA OPENCAGE AQUI!
OPENCAGE_BASE_URL = 'https://api.opencagedata.com/geocode/v1/json?'

# AwesomeAPI (para geocodificação de CEP primária)
AWESOMEAPI_BASE_URL_CEP = 'https://cep.awesomeapi.com.br/json/'

# Configurações do CSV (ajuste conforme seu arquivo)
CSV_DELIMITER = ';' # Mude para ',' se seu CSV usa vírgulas

# Nomes das colunas no seu CSV de entrada
ID_COLUMN = 'id'
NAME_COLUMN = 'nome'
CEP_COLUMN = 'cep' # Certifique-se que esta coluna existe no seu CSV de entrada
ADDRESS_COLUMN = 'endereco' # Certifique-se que esta coluna existe no seu CSV de entrada

# Contexto para melhorar a precisão da geocodificação de endereço (OpenCage)
DEFAULT_CITY_STATE_COUNTRY = ", Joinville, Santa Catarina, Brasil" 
# --- Fim Configurações ---

def geocode_with_awesomeapi(cep):
    """Geocodifica um CEP usando a AwesomeAPI e retorna (latitude, longitude)."""
    clean_cep = str(cep).replace('-', '').strip()
    if len(clean_cep) != 8:
        return None, None

    url = f"{AWESOMEAPI_BASE_URL_CEP}{clean_cep}"
    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status() 
        data = response.json()

        if data and 'lat' in data and 'lng' in data and data['lat'] != '0' and data['lng'] != '0':
            return float(data['lat']), float(data['lng'])
        else:
            # print(f"  AwesomeAPI: Coordenadas inválidas/não encontradas para CEP {clean_cep}. Resposta: {data}")
            return None, None
    except requests.exceptions.RequestException as e:
        # print(f"  AwesomeAPI: Erro de requisição para CEP {clean_cep}: {e}")
        return None, None
    except Exception as e:
        # print(f"  AwesomeAPI: Erro inesperado para CEP {clean_cep}: {e}")
        return None, None

def geocode_with_opencage(address, api_key):
    """Geocodifica um endereço usando a OpenCage API e retorna (latitude, longitude)."""
    if not address:
        return None, None

    params = {
        'q': address,
        'key': api_key,
        'language': 'pt',
        'pretty': 0,
        'no_annotations': 1
    }
    url = OPENCAGE_BASE_URL + urlencode(params)

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()

        if data and data['results'] and len(data['results']) > 0:
            lat = data['results'][0]['geometry']['lat']
            lon = data['results'][0]['geometry']['lng']
            return float(lat), float(lon)
        else:
            # print(f"  OpenCage: Coordenadas não encontradas para endereço '{address}'.")
            return None, None
    except requests.exceptions.RequestException as e:
        # print(f"  OpenCage: Erro de requisição para endereço '{address}': {e}")
        return None, None
    except Exception as e:
        # print(f"  OpenCage: Erro inesperado para endereço '{address}': {e}")
        return None, None

def main():
    if OPENCAGE_API_KEY == 'SUA_CHAVE_OPENCAGE':
        print("ERRO: Por favor, insira sua chave de API da OpenCage em OPENCAGE_API_KEY no script.")
        return

    print(f"Lendo dados de {INPUT_CSV_FILE}...")
    try:
        df = pd.read_csv(INPUT_CSV_FILE, sep=CSV_DELIMITER)
    except FileNotFoundError:
        print(f"Erro: O arquivo '{INPUT_CSV_FILE}' não foi encontrado. Verifique o nome e o caminho.")
        return
    except Exception as e:
        print(f"Erro ao ler o CSV. Verifique o delimitador (CSV_DELIMITER) e o formato do arquivo: {e}")
        return

    # Garante que as colunas de Latitude e Longitude existam
    if 'latitude' not in df.columns:
        df['latitude'] = None 
    if 'longitude' not in df.columns:
        df['longitude'] = None

    total_pdvs = len(df)
    print(f"Iniciando geocodificação para {total_pdvs} PDVs...")

    for index, row in df.iterrows():
        current_id = row.get(ID_COLUMN, 'N/A')
        current_name = row.get(NAME_COLUMN, 'N/A')
        current_cep = row.get(CEP_COLUMN, '')
        current_address = row.get(ADDRESS_COLUMN, '')

        lat, lon = None, None

        # 1. Tentar geocodificar usando CEP com AwesomeAPI
        if current_cep:
            lat, lon = geocode_with_awesomeapi(current_cep)
            if lat is not None:
                print(f"  [{current_id}] {current_name}: Geocodificado por CEP ({current_cep}): {lat}, {lon}")

        # 2. Se a geocodificação por CEP falhou, tentar usar o endereço com OpenCage
        if lat is None and current_address:
            query_address_opencage = f"{current_address}{DEFAULT_CITY_STATE_COUNTRY}".strip()
            lat, lon = geocode_with_opencage(query_address_opencage, OPENCAGE_API_KEY)
            if lat is not None:
                print(f"  [{current_id}] {current_name}: Geocodificado por Endereço OpenCage ('{current_address}'): {lat}, {lon}")

        if lat is None:
            print(f"  [{current_id}] {current_name}: *** Falha total na geocodificação (CEP: {current_cep}, Endereço: '{current_address}').")

        df.loc[index, 'latitude'] = lat
        df.loc[index, 'longitude'] = lon

        # Atraso para respeitar os limites de taxa da API
        # OpenCage: 1 requisição/segundo para camada gratuita
        # AwesomeAPI: Mais permissiva, mas um pequeno atraso é bom.
        time.sleep(1) 

    print(f"\nGeocodificação concluída. Salvando resultados em {OUTPUT_CSV_FILE}...")
    df.to_csv(OUTPUT_CSV_FILE, sep=CSV_DELIMITER, index=False, decimal='.') # Garante ponto decimal
    print("Done!")

if __name__ == "__main__":
    main()