"""
Script para validação do processo ETL
Verifica integridade dos dados processados
"""

import pandas as pd
from pathlib import Path

def validar_arquivos():
    """Valida se todos os arquivos foram criados corretamente"""
    database_path = Path("database")
    
    arquivos_esperados = [
        "empresas_final.csv",
        "estabelecimentos_final.csv", 
        "socios_final.csv",
        "empresas_final.parquet",
        "estabelecimentos_final.parquet",
        "socios_final.parquet"
    ]
    
    print("🔍 Validando arquivos...")
    
    for arquivo in arquivos_esperados:
        caminho = database_path / arquivo
        if caminho.exists():
            tamanho = caminho.stat().st_size / 1024 / 1024
            print(f"✅ {arquivo}: {tamanho:.2f} MB")
        else:
            print(f"❌ {arquivo}: Não encontrado")

def validar_dados():
    """Valida a qualidade dos dados"""
    database_path = Path("database")
    
    print("\n📊 Validando dados...")
    
    for arquivo in ["empresas_final.csv", "estabelecimentos_final.csv", "socios_final.csv"]:
        caminho = database_path / arquivo
        if caminho.exists():
            df = pd.read_csv(caminho, sep=';', nrows=1000)
            print(f"✅ {arquivo}: {len(df)} registros (amostra)")
        else:
            print(f"❌ {arquivo}: Não encontrado")

if __name__ == "__main__":
    validar_arquivos()
    validar_dados()
