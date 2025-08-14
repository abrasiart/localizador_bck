# backend/filtrar_pdv_produtos.py

import pandas as pd

# --- Configurações ---
PONTOS_DE_VENDA_FINAL_CSV = 'pontos_de_venda_final.csv'
PDV_PRODUTOS_ORIGINAL_CSV = 'pdv_produtos.csv'
PDV_PRODUTOS_FILTRADO_CSV = 'pdv_produtos_filtrado.csv'

CSV_DELIMITER = ';' # Mude para ',' se seus CSVs usam vírgulas
# --- Fim Configurações ---

def main():
    print(f"Lendo IDs de PDVs válidos de: {PONTOS_DE_VENDA_FINAL_CSV}")
    try:
        df_pontos_de_venda = pd.read_csv(PONTOS_DE_VENDA_FINAL_CSV, sep=CSV_DELIMITER, decimal='.', dtype={'id': str})
        valid_pdv_ids = set(df_pontos_de_venda['id'].astype(str).str.strip())
        print(f"Encontrados {len(valid_pdv_ids)} IDs de PDVs válidos.")
    except FileNotFoundError:
        print(f"Erro: O arquivo '{PONTOS_DE_VENDA_FINAL_CSV}' não foi encontrado. Certifique-se de que ele existe e está no caminho correto.")
        return
    except Exception as e:
        print(f"Erro ao ler '{PONTOS_DE_VENDA_FINAL_CSV}': {e}")
        return

    print(f"Lendo dados de ligação de: {PDV_PRODUTOS_ORIGINAL_CSV}")
    try:
        df_pdv_produtos = pd.read_csv(PDV_PRODUTOS_ORIGINAL_CSV, sep=CSV_DELIMITER, decimal='.', dtype={'pdv_id': str, 'produto_id': str})
        df_pdv_produtos['pdv_id'] = df_pdv_produtos['pdv_id'].str.replace('"', '').str.strip()
        df_pdv_produtos['produto_id'] = df_pdv_produtos['produto_id'].str.replace('"', '').str.strip()

        initial_rows = len(df_pdv_produtos)
        df_pdv_produtos.drop_duplicates(subset=['pdv_id', 'produto_id'], inplace=True)
        if len(df_pdv_produtos) < initial_rows:
            print(f"Removidas {initial_rows - len(df_pdv_produtos)} duplicatas internas em {PDV_PRODUTOS_ORIGINAL_CSV}.")

        print(f"Lidos {len(df_pdv_produtos)} registros de ligação.")
    except FileNotFoundError:
        print(f"Erro: O arquivo '{PDV_PRODUTOS_ORIGINAL_CSV}' não foi encontrado. Verifique o nome e o caminho.")
        return
    except Exception as e:
        print(f"Erro ao ler '{PDV_PRODUTOS_ORIGINAL_CSV}'. Verifique o delimitador e o formato: {e}")
        return

    df_pdv_produtos_filtrado = df_pdv_produtos[df_pdv_produtos['pdv_id'].isin(valid_pdv_ids)].copy()

    removed_rows = len(df_pdv_produtos) - len(df_pdv_produtos_filtrado)
    print(f"Registros removidos devido a PDV_ID ausente em pontos_de_venda: {removed_rows}")
    print(f"Total de registros de ligação válidos para salvar: {len(df_pdv_produtos_filtrado)}")

    df_pdv_produtos_filtrado.to_csv(PDV_PRODUTOS_FILTRADO_CSV, sep=CSV_DELIMITER, index=False, decimal='.')
    print(f"Arquivo filtrado salvo em: {PDV_PRODUTOS_FILTRADO_CSV}")
    print("Done!")

if __name__ == "__main__":
    main()