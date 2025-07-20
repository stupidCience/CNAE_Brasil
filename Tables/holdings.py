import duckdb
import pandas as pd

pd.set_option('display.max_columns', None)

socios_path = './database/socios_final.csv'
empresas_path = './database/empresas_final.csv'

query = f"""
SELECT e.cnpj_basico, e.razao_social, s.nome_socio, s.cnpj_cpf_socio
FROM read_csv_auto('{empresas_path}', 
    types={{'qualificacao_responsavel': 'VARCHAR'}}, 
    strict_mode=false, 
    null_padding=true
) e
LEFT JOIN read_csv_auto('{socios_path}') s
ON s.cnpj_basico = e.cnpj_basico
WHERE s.identificador_socio = 'PESSOA JURÍDICA'
"""

con = duckdb.connect()
result = con.execute(query).df()
result.to_csv('./database/holdings_result.csv', index=False, sep=';')
print(result.head())