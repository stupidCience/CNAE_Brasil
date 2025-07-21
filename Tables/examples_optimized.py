"""
Exemplos de consultas otimizadas usando DuckDB + Parquet
Performance muito superior aos CSVs tradicionais
"""

import duckdb
import pandas as pd

# Configuração global do pandas para mostrar todas as colunas
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

def grupos_empresariais():
    """Identifica grupos empresariais de forma otimizada"""
    
    query = """
    SELECT 
        s.cnpj_cpf_socio AS cnpj_controladora,
        s.nome_socio AS nome_controladora,
        COUNT(DISTINCT s.cnpj_basico) AS qtd_empresas_controladas,
        LISTAGG(DISTINCT e.razao_social, '; ') AS empresas_controladas,
        LISTAGG(DISTINCT s.cnpj_basico, '; ') AS cnpjs_controlados
    FROM 'database/socios_final.parquet' s
    JOIN 'database/empresas_final.parquet' e ON s.cnpj_basico = e.cnpj_basico
    WHERE s.identificador_socio = 'PESSOA JURÍDICA'
    GROUP BY s.cnpj_cpf_socio, s.nome_socio
    HAVING COUNT(DISTINCT s.cnpj_basico) >= 2
    ORDER BY qtd_empresas_controladas DESC
    LIMIT 100
    """
    
    result = duckdb.sql(query).df()
    print(f"🏢 Grupos empresariais encontrados: {len(result)}")
    return result

def empresas_por_porte_e_uf():
    """Análise por porte e localização usando joins otimizados"""
    
    query = """
    SELECT 
        est.uf,
        emp.porte_empresa,
        COUNT(*) as quantidade,
        ROUND(AVG(CAST(REPLACE(emp.capital_social, ',', '.') AS DOUBLE)), 2) as capital_medio
    FROM 'database/estabelecimentos_final.parquet' est
    JOIN 'database/empresas_final.parquet' emp ON LEFT(est.CNPJ, 8) = emp.cnpj_basico
    WHERE est.identificador_matriz = 'MATRIZ'
      AND emp.porte_empresa != 'NÃO INFORMADO'
      AND emp.capital_social IS NOT NULL
    GROUP BY est.uf, emp.porte_empresa
    ORDER BY est.uf, quantidade DESC
    """
    
    result = duckdb.sql(query).df()
    print(f"📊 Distribuição por UF e porte: {len(result)} registros")
    return result

def top_cnaes():
    """Top CNAEs por quantidade de empresas"""
    
    query = """
    SELECT 
        est.cnae_fiscal_principal,
        COUNT(*) as quantidade_empresas,
        COUNT(DISTINCT LEFT(est.CNPJ, 8)) as quantidade_cnpjs_basicos
    FROM 'database/estabelecimentos_final.parquet' est
    WHERE est.cnae_fiscal_principal IS NOT NULL
      AND est.situacao_cadastral = 'ATIVA'
    GROUP BY est.cnae_fiscal_principal
    ORDER BY quantidade_empresas DESC
    LIMIT 50
    """
    
    result = duckdb.sql(query).df()
    print(f"🎯 Top CNAEs: {len(result)} categorias")
    return result

if __name__ == "__main__":
    print("🚀 Executando consultas otimizadas...")
    
    print("\n1️⃣ Grupos Empresariais:")
    grupos = grupos_empresariais()
    print(grupos.head())
    
    print("\n2️⃣ Empresas por Porte e UF:")
    porte_uf = empresas_por_porte_e_uf()
    print(porte_uf.head(10))
    
    print("\n3️⃣ Top CNAEs:")
    cnaes = top_cnaes()
    print(cnaes.head(10))
