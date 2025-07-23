"""
Script para análise e validação dos dados antes da inserção
Uso: python scripts/analyze_data.py
"""

import sys
import os
from pathlib import Path
import duckdb as db
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parent.parent))

def analisar_tamanhos_campos():
    """Analisa os tamanhos dos campos para otimizar schema do banco"""
    print("📊 Analisando tamanhos dos campos...")
    
    parquet_file = Path("database/empresas_final.parquet")
    if not parquet_file.exists():
        print("❌ Arquivo empresas_final.parquet não encontrado")
        return
    
    con = db.connect()
    
    # Análise de tamanhos de campos texto
    queries = {
        'razao_social': "SELECT MAX(LENGTH(razao_social)) as max_len FROM 'database/empresas_final.parquet'",
        'natureza_juridica': "SELECT MAX(LENGTH(natureza_juridica)) as max_len FROM 'database/empresas_final.parquet'",
        'qualificacao_responsavel': "SELECT MAX(LENGTH(qualificacao_responsavel)) as max_len FROM 'database/empresas_final.parquet'",
        'porte_empresa': "SELECT MAX(LENGTH(porte_empresa)) as max_len FROM 'database/empresas_final.parquet'"
    }
    
    print("\n📏 Tamanhos máximos dos campos:")
    for campo, query in queries.items():
        result = con.execute(query).fetchone()
        print(f"  {campo}: {result[0]} caracteres")
    
    # Contagem de registros
    count_query = "SELECT COUNT(*) FROM 'database/empresas_final.parquet'"
    total = con.execute(count_query).fetchone()[0]
    print(f"\n📈 Total de registros: {total:,}")
    
    # Amostra dos dados
    sample_query = "SELECT * FROM 'database/empresas_final.parquet' LIMIT 5"
    df_sample = con.execute(sample_query).fetchdf()
    print("\n🔍 Amostra dos dados:")
    print(df_sample.head())
    
    con.close()

def verificar_qualidade_dados():
    """Verifica qualidade e consistência dos dados"""
    print("\n🔍 Verificando qualidade dos dados...")
    
    con = db.connect()
    
    # Valores nulos
    null_query = """
    SELECT 
        COUNT(*) as total,
        SUM(CASE WHEN cnpj_basico IS NULL THEN 1 ELSE 0 END) as cnpj_null,
        SUM(CASE WHEN razao_social IS NULL THEN 1 ELSE 0 END) as razao_null,
        SUM(CASE WHEN natureza_juridica IS NULL THEN 1 ELSE 0 END) as natureza_null,
        SUM(CASE WHEN qualificacao_responsavel IS NULL THEN 1 ELSE 0 END) as qualif_null
    FROM 'database/empresas_final.parquet'
    """
    
    result = con.execute(null_query).fetchone()
    print(f"  Total de registros: {result[0]:,}")
    print(f"  CNPJ nulos: {result[1]:,}")
    print(f"  Razão social nula: {result[2]:,}")
    print(f"  Natureza jurídica nula: {result[3]:,}")
    print(f"  Qualificação nula: {result[4]:,}")
    
    # Duplicatas
    dup_query = """
    SELECT COUNT(*) - COUNT(DISTINCT cnpj_basico) as duplicatas
    FROM 'database/empresas_final.parquet'
    """
    duplicatas = con.execute(dup_query).fetchone()[0]
    print(f"  CNPJs duplicados: {duplicatas:,}")
    
    con.close()

def analisar_memoria():
    """Analisa uso de memória para determinar chunk size"""
    print("\n💾 Analisando uso de memória...")
    
    parquet_file = Path("database/empresas_final.parquet")
    tamanho_mb = parquet_file.stat().st_size / (1024 * 1024)
    print(f"  Tamanho do arquivo: {tamanho_mb:.1f} MB")
    
    con = db.connect()
    
    # Estimar registros
    count_query = "SELECT COUNT(*) FROM 'database/empresas_final.parquet'"
    total_registros = con.execute(count_query).fetchone()[0]
    
    # Calcular chunk size recomendado
    mb_por_registro = tamanho_mb / total_registros
    registros_por_100mb = int(100 / mb_por_registro)
    
    print(f"  Total de registros: {total_registros:,}")
    print(f"  MB por registro: {mb_por_registro:.6f}")
    print(f"  Chunk size recomendado (100MB): {registros_por_100mb:,}")
    
    con.close()

def main():
    """Função principal"""
    print("🔍 Análise de dados para inserção no banco MySQL")
    print("=" * 50)
    
    analisar_tamanhos_campos()
    verificar_qualidade_dados()
    analisar_memoria()
    
    print("\n✅ Análise concluída!")

if __name__ == "__main__":
    main()
