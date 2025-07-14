import pandas as pd

try:
    # Lendo o arquivo de sócios
    socios = pd.read_parquet("./database/socios.parquet")

    matrizes = pd.read_parquet(
        "./database/estabelecimentos.parquet",
        columns=[
            'CNPJ',
            'nome_fantasia',
            'identificador_matriz'
        ],
        filters=[('identificador_matriz', '==', 'MATRIZ')]
    )

    # Garante que a coluna 'CNPJ' seja do tipo string antes de fatiar
    matrizes['CNPJ'] = matrizes['CNPJ'].astype(str)

    # Cria a coluna 'cnpj_basico' extraindo os 8 primeiros dígitos do CNPJ
    matrizes['cnpj_basico'] = matrizes['CNPJ'].str[:8]

    # Realiza o merge dos DataFrames 'matrizes' e 'socios'
    # 'left_on' e 'right_on' especificam as colunas para o merge em cada DataFrame.
    # 'how='left'' garante que todas as matrizes sejam mantidas, mesmo sem sócios correspondentes.
    # 'suffixes' adiciona sufixos para diferenciar colunas com o mesmo nome que venham de 'socios'.
    matrizes = matrizes.merge(
        socios,
        left_on='cnpj_basico',
        right_on='cnpj_basico',
        how='left',
        suffixes=('', '_socio') # Ex: se 'nome' existe em ambos, o de socios vira 'nome_socio'
    )

    # Configura a opção de exibição do pandas para mostrar todas as colunas
    pd.set_option('display.max_columns', None)

    # Filtra as matrizes onde a 'qualificacao_representante' é 'SÓCIO'
    # Consideração: Certifique-se de que 'qualificacao_representante' existe após o merge
    # e que o valor 'SÓCIO' está capitalizado corretamente.

    # Imprime as primeiras 5 linhas do DataFrame resultante
    print("DataFrame 'matrizes' após filtros e merge (primeiras 5 linhas):")
    print(matrizes.head())

except FileNotFoundError as e:
    print(f"Erro: Um ou ambos os arquivos Parquet não foram encontrados no caminho especificado.")
    print(f"Por favor, verifique se os arquivos 'socios.parquet' e 'estabelecimentos.parquet' estão em './database/'.")
    print(f"Detalhes do erro: {e}")
except KeyError as e:
    print(f"Erro: Uma coluna esperada não foi encontrada após a leitura ou merge.")
    print(f"Verifique se as colunas '{e}' existem nos seus arquivos Parquet ou no DataFrame resultante do merge.")
    print(f"Detalhes do erro: {e}")
except Exception as e:
    print(f"Ocorreu um erro inesperado: {e}")