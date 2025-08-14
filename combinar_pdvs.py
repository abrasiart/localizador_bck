# backend/combinar_pdvs.py

import pandas as pd

# --- Configurações ---
# Nome do arquivo CSV com os PDVs que já estavam corretos
CSV_CORRETOS = 'pdvs_1_com_coords.csv' 
# Nome do arquivo CSV que você acabou de gerar/corrigir
CSV_NOVOS_CORRIGIDOS = 'pdvs_2_com_coords.csv' 
# Nome do arquivo CSV de saída, com todos os PDVs combinados
CSV_FINAL_COMBINADO = 'pontos_de_venda_final.csv' 

CSV_DELIMITER = ';' # Mude para ',' se seus CSVs usam vírgulas
# --- Fim Configurações ---

def main():
    try:
        # Lendo o primeiro arquivo
        df_corretos = pd.read_csv(CSV_CORRETOS, sep=CSV_DELIMITER, decimal='.') # Garante leitura correta
        print(f"Lido {len(df_corretos)} PDVs do arquivo: {CSV_CORRETOS}")
    except FileNotFoundError:
        print(f"Aviso: O arquivo '{CSV_CORRETOS}' não foi encontrado. Prosseguindo apenas com o novo arquivo.")
        df_corretos = pd.DataFrame() # Cria um DataFrame vazio se o arquivo não existir

    try:
        # Lendo o segundo arquivo (o que acabou de ser processado/corrigido)
        df_novos = pd.read_csv(CSV_NOVOS_CORRIGIDOS, sep=CSV_DELIMITER, decimal='.') # Garante leitura correta
        print(f"Lido {len(df_novos)} PDVs do arquivo: {CSV_NOVOS_CORRIGIDOS}")
    except FileNotFoundError:
        print(f"Erro: O arquivo '{CSV_NOVOS_CORRIGIDOS}' não foi encontrado. Não é possível combinar.")
        return

    # Combinar os DataFrames
    # Primeiro, colocamos os dados corrigidos/novos (df_novos)
    # Depois, os dados que já estavam corretos (df_corretos)
    # O drop_duplicates com keep='first' vai garantir que, se um ID existir em ambos,
    # a versão do 'df_novos' (que vem primeiro) será mantida, o que é o que queremos
    # se 'df_novos' contém as versões corrigidas.
    df_combinado = pd.concat([df_novos, df_corretos]).drop_duplicates(subset=['id'], keep='first')

    # Opcional: Reordenar as colunas se o concat as bagunçar
    # Colunas esperadas: id, nome, cep, endereco, latitude, longitude
    expected_columns = ['id', 'nome', 'cep', 'endereco', 'latitude', 'longitude']
    df_combinado = df_combinado[expected_columns]


    print(f"\nTotal de PDVs combinados (sem duplicatas): {len(df_combinado)}")

    # Salvar o arquivo CSV final combinado
    df_combinado.to_csv(CSV_FINAL_COMBINADO, sep=CSV_DELIMITER, index=False, decimal='.')
    print(f"Dados combinados salvos em: {CSV_FINAL_COMBINADO}")

if __name__ == "__main__":
    main()