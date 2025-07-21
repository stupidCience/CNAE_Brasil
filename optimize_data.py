"""
Script para otimização de dados - Converte CSVs para Parquet
Melhora drasticamente a performance de consultas e reduz tamanho dos arquivos
"""

import duckdb
import time
from pathlib import Path
import os
import chardet

def detect_file_encoding(file_path: str) -> str:
    """Detecta o encoding de um arquivo CSV e retorna formato compatível com DuckDB"""
    try:
        with open(file_path, 'rb') as f:
            # Lê uma amostra do arquivo para detectar encoding
            sample = f.read(100000)  # 100KB
            result = chardet.detect(sample)
            detected_encoding = result['encoding']
            confidence = result['confidence']
            
            print(f"   🔍 Encoding detectado: {detected_encoding} (confiança: {confidence:.2f})")
            
            # Mapeia encodings para formatos suportados pelo DuckDB
            if detected_encoding:
                detected_lower = detected_encoding.lower()
                if 'utf' in detected_lower and '8' in detected_lower:
                    return 'UTF-8'  # DuckDB usa UTF-8 com hífen
                elif 'latin' in detected_lower or 'iso-8859-1' in detected_lower:
                    return 'ISO-8859-1'  # DuckDB prefere ISO-8859-1
                elif 'cp1252' in detected_lower or 'windows-1252' in detected_lower:
                    return 'windows-1252'
                else:
                    return 'UTF-8'  # Default seguro
            else:
                return 'UTF-8'  # Default
                
    except Exception as e:
        print(f"   ⚠️ Erro na detecção de encoding: {e}")
        return 'UTF-8'  # Default

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
                # Detecta encoding automaticamente
                encoding = detect_file_encoding(str(csv_path))
                
                # DuckDB é mais eficiente para esta conversão
                start_time = time.time()
                
                # Instala extensões necessárias
                duckdb.sql("INSTALL encodings; LOAD encodings")
                
                # Inicializa variável de controle
                success = False
                
                # Tenta conversão simples sem especificar encoding primeiro
                try:
                    print(f"   🧪 Tentando sem especificar encoding (auto-detect)")
                    
                    duckdb.sql(f"""
                        COPY (
                            SELECT * FROM read_csv_auto('{csv_path}', 
                                sep=';',
                                ignore_errors=true,
                                null_padding=true,
                                max_line_size=1000000,
                                strict_mode=false
                            )
                        ) TO '{parquet_path}' (FORMAT PARQUET, COMPRESSION 'snappy')
                    """)
                    
                    success = True
                    print(f"   ✅ Sucesso com auto-detect")
                    
                except Exception as e:
                    print(f"   ❌ Auto-detect falhou: {str(e)[:80]}...")
                    
                    # Se auto-detect falhar, tenta encodings específicos
                    if 'socios' in csv_file:
                        # Para sócios, tenta latin-1 primeiro (problema conhecido com caracteres especiais)
                        encodings_to_try = ['latin-1', 'CP1252', 'windows-1252']
                    else:
                        # Para outros arquivos, UTF-8 primeiro
                        encodings_to_try = ['UTF-8', 'latin-1', 'CP1252']
                            
                        for enc in encodings_to_try:
                            try:
                                print(f"   🧪 Tentando encoding: {enc}")
                                
                                # Para arquivos problemáticos, usa configurações mais tolerantes
                                if 'socios' in csv_file and enc == 'latin-1':
                                    duckdb.sql(f"""
                                        COPY (
                                            SELECT * FROM read_csv_auto('{csv_path}', 
                                                sep=';',
                                                encoding='{enc}',
                                                ignore_errors=true,
                                                null_padding=true,
                                                max_line_size=2000000,
                                                strict_mode=false,
                                                sample_size=100000
                                            )
                                        ) TO '{parquet_path}' (FORMAT PARQUET, COMPRESSION 'snappy')
                                    """)
                                else:
                                    duckdb.sql(f"""
                                        COPY (
                                            SELECT * FROM read_csv_auto('{csv_path}', 
                                                sep=';',
                                                encoding='{enc}',
                                                ignore_errors=true,
                                                null_padding=true,
                                                max_line_size=1000000,
                                                strict_mode=false
                                            )
                                        ) TO '{parquet_path}' (FORMAT PARQUET, COMPRESSION 'snappy')
                                    """)
                                
                                success = True
                                print(f"   ✅ Sucesso com encoding: {enc}")
                                break
                                
                            except Exception as e:
                                error_msg = str(e)
                                if 'encoding' in error_msg.lower():
                                    print(f"   ❌ Falhou com {enc}: Encoding não suportado")
                                elif 'decode' in error_msg.lower():
                                    print(f"   ❌ Falhou com {enc}: Erro de decodificação")
                                else:
                                    print(f"   ❌ Falhou com {enc}: {error_msg[:60]}...")
                                
                                # Remove arquivo parcial se existir
                                if Path(parquet_path).exists():
                                    Path(parquet_path).unlink()
                                continue
                            continue
                
                if not success:
                    print(f"   ❌ Erro na conversão: Nenhum encoding funcionou")
                    continue
                
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
    
    # Instala extensões necessárias
    try:
        duckdb.sql("INSTALL encodings; LOAD encodings")
    except:
        pass
    
    # Detecta encoding do arquivo empresas
    empresas_csv = Path("database/empresas_final.csv")
    encoding = 'UTF-8'
    if empresas_csv.exists():
        encoding = detect_file_encoding(str(empresas_csv))
    
    # Testes de performance
    tests = [
        {
            'name': 'Contagem simples',
            'query_csv': f"SELECT COUNT(*) FROM read_csv_auto('database/empresas_final.csv', sep=';', encoding='{encoding}', strict_mode=false, null_padding=true, ignore_errors=true)",
            'query_parquet': "SELECT COUNT(*) FROM 'database/empresas_final.parquet'"
        },
        {
            'name': 'Filtro por porte',
            'query_csv': f"SELECT COUNT(*) FROM read_csv_auto('database/empresas_final.csv', sep=';', encoding='{encoding}', strict_mode=false, null_padding=true, ignore_errors=true) WHERE porte_empresa = 'MICRO EMPRESA'",
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
            
            # Parquet (só se existir)
            parquet_path = Path("database/empresas_final.parquet")
            if parquet_path.exists():
                start = time.time()  
                result_parquet = duckdb.sql(test['query_parquet']).fetchone()[0]
                parquet_time = time.time() - start
                
                speedup = csv_time / parquet_time if parquet_time > 0 else float('inf')
                print(f"   CSV: {csv_time:.2f}s (resultado: {result_csv:,})")
                print(f"   Parquet: {parquet_time:.2f}s (resultado: {result_parquet:,})")
                print(f"   🚀 Speedup: {speedup:.1f}x mais rápido")
            else:
                print(f"   CSV: {csv_time:.2f}s (resultado: {result_csv:,})")
                print(f"   ❌ Parquet não encontrado")
            
        except Exception as e:
            print(f"   ❌ Erro no benchmark: {str(e)[:200]}...")

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
