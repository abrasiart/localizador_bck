# backend/excluir_produtos_pdv.py

import pandas as pd

# --- Configurações ---
INPUT_CSV_FILE = 'pdv_produtos_filtrado.csv' # Arquivo CSV de entrada a ser filtrado
OUTPUT_CSV_FILE = 'pdv_produtos_filtrado_final.csv' # Nome do novo arquivo CSV de saída

CSV_DELIMITER = ';' # Mude para ',' se seu CSV usa vírgulas

# LISTA DE IDS DE PRODUTOS A SEREM EXCLUÍDOS
# Substitua estes IDs pelos IDs dos produtos que você deseja remover da lista de PDVs
PRODUTO_IDS_A_EXCLUIR = [
'01804',
'03000',
'03001',
'03002',
'03004',
'03005',
'03006',
'03009',
'03009',
'03011',
'03012',
'03013',
'03014',
'03015',
'03016',
'03017',
'03018',
'03019',
'03020',
'03020',
'03021',
'03023',
'03023',
'03024',
'03025',
'03026',
'03027',
'03028',
'03028',
'03029',
'03030',
'03031',
'03031',
'03032',
'03032',
'03035',
'03037',
'03038',
'03039',
'03040',
'03041',
'03045',
'03047',
'03049',
'03052',
'03055',
'03056',
'03056',
'03057',
'03061',
'03061',
'03063',
'03066',
'03069',
'03069',
'03070',
'03072',
'03072',
'03073',
'03073',
'03075',
'03076',
'03077',
'03078',
'03079',
'03080',
'03081',
'03081',
'03082',
'03083',
'03085',
'03086',
'03087',
'03088',
'03089',
'03090',
'03091',
'03091',
'03092',
'03093',
'03094',
'03095',
'03097',
'03098',
'03099',
'03100',
'03101',
'03102',
'03103',
'03104',
'03105',
'03107',
'03108',
'03109',
'03110',
'03111',
'03112',
'03113',
'03114',
'03115',
'03116',
'03117',
'03118',
'03119',
'03120',
'042513',
'042513',
'07977',
'07978',
'07979',
'07980',
'07981',
'07999',
'91251',
'91252',
'91253',
'10211',
'10212',
'30105',
'40091',
'40092',
'40101',
'40102',
'40122',
'42508',
'42509',
'42510',
'42511',
'42512',
'42513',
'91234',
'91236',
'91237',
'91235',
'91424',
'91525',
'91526',
'91635',
'91637',
'91706',
'91707',
'91708'



]
# --- Fim Configurações ---

def main():
    print(f"Lendo dados de {INPUT_CSV_FILE} para filtrar...")
    try:
        # Lê o CSV, garantindo que pdv_id e produto_id são lidos como strings
        df = pd.read_csv(INPUT_CSV_FILE, sep=CSV_DELIMITER, dtype={'pdv_id': str, 'produto_id': str})
        
        # Opcional: Remover aspas duplas e espaços em branco que possam ter sobrado
        df['pdv_id'] = df['pdv_id'].str.replace('"', '').str.strip()
        df['produto_id'] = df['produto_id'].str.replace('"', '').str.strip()
        
        print(f"Lidas {len(df)} linhas do arquivo original.")
    except FileNotFoundError:
        print(f"Erro: O arquivo '{INPUT_CSV_FILE}' não foi encontrado. Verifique o nome e o caminho.")
        return
    except Exception as e:
        print(f"Erro ao ler o CSV. Verifique o delimitador (CSV_DELIMITER) e o formato do arquivo: {e}")
        return

    # Filtra o DataFrame para REMOVER as linhas onde 'produto_id' está na lista de exclusão
    df_filtrado = df[~df['produto_id'].isin(PRODUTO_IDS_A_EXCLUIR)].copy()

    removidas = len(df) - len(df_filtrado)
    print(f"Removidas {removidas} linhas com os produtos especificados.")
    print(f"Total de linhas restantes para salvar: {len(df_filtrado)}")

    # Salva o novo arquivo CSV filtrado
    # 'decimal='.'': Garante que números (se houver no futuro) usem ponto como decimal
    df_filtrado.to_csv(OUTPUT_CSV_FILE, sep=CSV_DELIMITER, index=False, decimal='.')
    print(f"Dados filtrados salvos em: {OUTPUT_CSV_FILE}")
    print("Done!")

if __name__ == "__main__":
    main()