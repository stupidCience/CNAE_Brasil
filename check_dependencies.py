#!/usr/bin/env python3
"""
Script de verificação e instalação de dependências
Para o projeto CNAE Brasil
"""

import subprocess
import sys

def check_package(package_name):
    """Verifica se um pacote está instalado"""
    try:
        __import__(package_name)
        return True
    except ImportError:
        return False

def main():
    """Verifica dependências principais"""
    
    print("🔍 Verificando dependências do projeto CNAE Brasil...")
    print("=" * 60)
    
    # Lista de dependências principais
    dependencies = [
        ('pandas', 'pandas'),
        ('duckdb', 'duckdb'), 
        ('requests', 'requests'),
        ('tqdm', 'tqdm'),
        ('pyarrow', 'pyarrow'),
        ('chardet', 'chardet'),
        ('dateutil', 'python-dateutil')
    ]
    
    missing = []
    
    for import_name, pip_name in dependencies:
        if check_package(import_name):
            print(f"✅ {pip_name}")
        else:
            print(f"❌ {pip_name} - FALTANDO")
            missing.append(pip_name)
    
    print("=" * 60)
    
    if missing:
        print(f"⚠️  {len(missing)} dependência(s) faltando:")
        for pkg in missing:
            print(f"   - {pkg}")
        print()
        print("💡 Para instalar as dependências faltantes, execute:")
        print("   pip install -r requirements.txt")
    else:
        print("🎉 Todas as dependências estão instaladas!")
        print()
        print("🚀 Pronto para executar:")
        print("   python main_etl.py        # ETL completo")
        print("   python optimize_data.py   # Otimização Parquet")
        print("   python validate_etl.py    # Validação de dados")

if __name__ == "__main__":
    main()
