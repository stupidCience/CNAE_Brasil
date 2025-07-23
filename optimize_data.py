"""
Script para otimização de dados - Converte CSVs para Parquet
Melhora drasticamente a performance de consultas e reduz tamanho dos arquivos
"""

import duckdb
import pandas as pd
import time
from pathlib import Path
import os

def convert_to_parquet():
    """Converte CSVs finais para Parquet para consultas eficientes"""
    
    base_path = Path("database")
    
    conversions = [
        ("empresas_final.csv", "empresas_final.parquet"),
        ("estabelecimentos_final.csv", "estabelecimentos_final.parquet"), 
        ("socios_final.csv", "socios_final.parquet")
    ]
    
    total_csv_size = 0
    total_parquet_size = 0
    
    for csv_file, parquet_file in conversions:
        csv_path = base_path / csv_file
        parquet_path = base_path / parquet_file
        
        if csv_path.exists():
            print(f"🔄 Convertendo {csv_file} → {parquet_file}...")
            
            try:
                # DuckDB é mais eficiente para esta conversão
                start_time = time.time()
                
                # Configurações específicas por tipo de arquivo
                if "empresas" in csv_file:
                    # Para empresas - problema de colunas desbalanceadas
                    duckdb.sql(f"""
                        COPY (SELECT * FROM read_csv_auto('{csv_path}', 
                            sep=';', 
                            strict_mode=false, 
                            null_padding=true,
                            ignore_errors=true
                        )) 
                        TO '{parquet_path}' (FORMAT PARQUET)
                    """)
                elif "socios" in csv_file:
                    # Para sócios - problema de encoding
                    duckdb.sql(f"""
                        COPY (SELECT * FROM read_csv_auto('{csv_path}', 
                            sep=';',
                            encoding='ISO-8859-1',
                            strict_mode=false,
                            null_padding=true,
                            ignore_errors=true
                        )) 
                        TO '{parquet_path}' (FORMAT PARQUET)
                    """)
                else:
                    # Para estabelecimentos (já funcionando)
                    duckdb.sql(f"""
                        COPY (SELECT * FROM read_csv_auto('{csv_path}', sep=';')) 
                        TO '{parquet_path}' (FORMAT PARQUET)
                    """)
                
                conversion_time = time.time() - start_time
                
                # Comparar tamanhos
                csv_size = csv_path.stat().st_size / (1024**2)  # MB
                parquet_size = parquet_path.stat().st_size / (1024**2)  # MB
                compression = (1 - parquet_size/csv_size) * 100
                
                total_csv_size += csv_size
                total_parquet_size += parquet_size
                
                print(f"   ✅ {csv_size:.1f}MB → {parquet_size:.1f}MB (compressão: {compression:.1f}%) em {conversion_time:.1f}s")
                
            except Exception as e:
                print(f"   ❌ Erro na conversão: {str(e)[:100]}...")
        else:
            print(f"   ⚠️  Arquivo {csv_file} não encontrado")
    
    if total_csv_size > 0:
        total_compression = (1 - total_parquet_size/total_csv_size) * 100
        print(f"\n📊 Total: {total_csv_size:.1f}MB → {total_parquet_size:.1f}MB (economia: {total_compression:.1f}%)")

def benchmark_queries():
    """Compara performance entre CSV e Parquet"""
    
    print("\n🏃 Executando benchmark de performance...")
    
    # Testes de performance
    tests = [
        {
            'name': 'Contagem simples',
            'query_csv': "SELECT COUNT(*) FROM read_csv_auto('database/empresas_final.csv', sep=';')",
            'query_parquet': "SELECT COUNT(*) FROM 'database/empresas_final.parquet'"
        },
        {
            'name': 'Filtro por porte',
            'query_csv': "SELECT COUNT(*) FROM read_csv_auto('database/empresas_final.csv', sep=';') WHERE porte_empresa = 'MICRO EMPRESA'",
            'query_parquet': "SELECT COUNT(*) FROM 'database/empresas_final.parquet' WHERE porte_empresa = 'MICRO EMPRESA'"
        }
    ]
    
    for test in tests:
        print(f"\n📊 Teste: {test['name']}")
        
        try:
            # CSV
            start = time.time()
            result_csv = duckdb.sql(test['query_csv']).fetchone()[0]
            csv_time = time.time() - start
            
            # Parquet
            start = time.time()  
            result_parquet = duckdb.sql(test['query_parquet']).fetchone()[0]
            parquet_time = time.time() - start
            
            if csv_time > 0:
                speedup = csv_time / parquet_time
                print(f"   CSV: {csv_time:.2f}s (resultado: {result_csv:,})")
                print(f"   Parquet: {parquet_time:.2f}s (resultado: {result_parquet:,})")
                print(f"   🚀 Speedup: {speedup:.1f}x mais rápido")
            
        except Exception as e:
            print(f"   ❌ Erro no benchmark: {e}")

def create_optimized_queries_examples():
    """Cria exemplos de consultas otimizadas usando DuckDB + Parquet"""
    
    examples_path = Path("Tables/examples_optimized.py")
    
    example_code = '''"""
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
    
    print("\\n1️⃣ Grupos Empresariais:")
    grupos = grupos_empresariais()
    print(grupos.head())
    
    print("\\n2️⃣ Empresas por Porte e UF:")
    porte_uf = empresas_por_porte_e_uf()
    print(porte_uf.head(10))
    
    print("\\n3️⃣ Top CNAEs:")
    cnaes = top_cnaes()
    print(cnaes.head(10))
'''
    
    with open(examples_path, 'w', encoding='utf-8') as f:
        f.write(example_code)
    
    print(f"📝 Exemplos de consultas otimizadas criados em: {examples_path}")

def validate_parquet_files():
    """Valida se os arquivos Parquet foram criados corretamente"""
    
    print("\n✅ Validando arquivos Parquet...")
    
    parquet_files = [
        "database/empresas_final.parquet",
        "database/estabelecimentos_final.parquet", 
        "database/socios_final.parquet"
    ]
    
    for file_path in parquet_files:
        if Path(file_path).exists():
            try:
                # Testa leitura
                result = duckdb.sql(f"SELECT COUNT(*) FROM '{file_path}'").fetchone()[0]
                print(f"   ✅ {file_path}: {result:,} registros")
            except Exception as e:
                print(f"   ❌ {file_path}: Erro na validação - {e}")
        else:
            print(f"   ❌ {file_path}: Arquivo não encontrado")

if __name__ == "__main__":
    print("🚀 Iniciando otimização dos dados...")
    convert_to_parquet()
    benchmark_queries()
    create_optimized_queries_examples()
    validate_parquet_files()
    print("\n🎉 Otimização concluída!")
