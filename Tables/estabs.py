import duckdb
import pandas as pd
pd.set_option('display.max_columns', None)

estabs_path = './database/estabelecimentos_final.csv'

query = f"""
SELECT 
    LEFT(estabs.CNPJ, 8) AS cnpj_basico,
    estabs.CNPJ,
    estabs.identificador_matriz,
    estabs.nome_fantasia,
    matriz.nome_fantasia AS nome_fantasia_matriz,
    estabs.situacao_cadastral,
    estabs.cnae_fiscal_principal
FROM read_csv_auto('{estabs_path}',
    types={{
        'ddd1': 'VARCHAR',
        'telefone1': 'VARCHAR',
        'ddd2': 'VARCHAR',
        'telefone2': 'VARCHAR',
        'dddfax': 'VARCHAR',
        'fax': 'VARCHAR',
        'cep': 'VARCHAR'
    }},
    strict_mode=false,
    null_padding=true,
    parallel=false
) AS estabs
JOIN (
        SELECT
            LEFT(CNPJ, 8) AS cnpj_basico,
            CNPJ,
            nome_fantasia
        FROM
            read_csv_auto('{estabs_path}',
            types={{
                'ddd1': 'VARCHAR',
                'telefone1': 'VARCHAR',
                'ddd2': 'VARCHAR',
                'telefone2': 'VARCHAR',
                'dddfax': 'VARCHAR',
                'fax': 'VARCHAR',
                'cep': 'VARCHAR'
            }},
            strict_mode=false,
            null_padding=true,
            parallel=false
        ) 
        WHERE
            identificador_matriz = 'MATRIZ'
) AS matriz
ON matriz.cnpj_basico = LEFT(estabs.CNPJ, 8)
WHERE
    estabs.identificador_matriz = 'FILIAL'
"""

con = duckdb.connect()
result = con.execute(query).df()
result.to_csv('./database/matrizes_e_filiais.csv', index=False, sep=';')
print(result.head())
