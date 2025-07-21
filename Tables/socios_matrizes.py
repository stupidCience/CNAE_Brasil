import duckdb
import pandas as pd
import os

# Garante que o diretório existe
os.makedirs('./database', exist_ok=True)

pd.set_option('display.max_columns', None)

print("🚀 Processando grupos empresariais usando DuckDB + Parquet (otimizado)...")

# Query otimizada usando Parquet - MUITO mais rápida que CSV
query = """
SELECT 
    e.cnpj_basico,
    e.razao_social AS Razao_Social,
    s.nome_socio AS Nome_Grupo_empresarial,
    s.cnpj_cpf_socio AS CNPJ_Controladora,
    matriz.nome_fantasia AS Nome_Matriz,
    e.capital_social AS Faturamento_estimado,
    grupo_info.CNPJs_No_grupo
FROM 'database/empresas_final.parquet' e
LEFT JOIN 'database/socios_final.parquet' s 
    ON e.cnpj_basico = s.cnpj_basico 
    AND s.identificador_socio = 'PESSOA JURÍDICA'
LEFT JOIN 'database/estabelecimentos_final.parquet' matriz
    ON e.cnpj_basico = LEFT(matriz.CNPJ, 8)
    AND matriz.identificador_matriz = 'MATRIZ'
LEFT JOIN (
    SELECT 
        s_inner.cnpj_cpf_socio,
        LISTAGG(s_inner.cnpj_basico, ';') AS CNPJs_No_grupo
    FROM 'database/socios_final.parquet' s_inner
    WHERE s_inner.identificador_socio = 'PESSOA JURÍDICA'
    GROUP BY s_inner.cnpj_cpf_socio
) grupo_info ON s.cnpj_cpf_socio = grupo_info.cnpj_cpf_socio
WHERE s.identificador_socio = 'PESSOA JURÍDICA'
LIMIT 10000
"""

print("⚡ Executando consulta otimizada...")
con = duckdb.connect()
result = con.execute(query).df()

print(f"📊 Resultado: {len(result)} registros encontrados")

if not result.empty:
    result.to_csv('./database/matrizes_e_filiais_grupos_pandas.csv', index=False, sep=';')
    print("✅ Arquivo exportado para ./database/matrizes_e_filiais_grupos_pandas.csv")
    print("\n📋 Amostra dos resultados:")
    print(result.head())
else:
    print("⚠️ Nenhum resultado encontrado.")

print("\n🎉 Processamento concluído!")
