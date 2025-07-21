"""
Configurações centralizadas para o projeto CNAE
Define paths, schemas e configurações padrão
"""

import os
from pathlib import Path

# Diretório base do projeto
PROJECT_ROOT = Path(__file__).resolve().parent

# Configurações de diretórios
DIRS = {
    'data': PROJECT_ROOT / 'Data',
    'database': PROJECT_ROOT / 'database', 
    'auxiliar': PROJECT_ROOT / 'Auxiliar',
    'schemas': PROJECT_ROOT / 'Schemas',
    'services': PROJECT_ROOT / 'Services',
    'process': PROJECT_ROOT / 'process',
    'tables': PROJECT_ROOT / 'Tables'
}

# Configurações de processamento
PROCESSING = {
    'chunk_size': 100_000,
    'encoding_br': 'latin1',  # Encoding para arquivos brasileiros
    'encoding_utf8': 'utf-8',
    'csv_separator': ';'
}

# URLs base para download
RECEITA_BASE_URL = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj"

# Padrões de arquivos
FILE_PATTERNS = {
    'empresas': 'Empresas*.zip',
    'estabelecimentos': 'Estabelecimentos*.zip', 
    'socios': 'Socios*.zip'
}

# Configurações de validação
VALIDATION = {
    'max_sample_size': 10000,  # Tamanho da amostra para validação
    'required_files': [
        'empresas_final.csv',
        'estabelecimentos_final.csv', 
        'socios_final.csv'
    ]
}

def ensure_directories():
    """Cria todos os diretórios necessários se não existirem"""
    for dir_path in DIRS.values():
        dir_path.mkdir(parents=True, exist_ok=True)

def get_auxiliar_files():
    """Retorna dict com paths dos arquivos auxiliares"""
    return {
        'naturezas': DIRS['auxiliar'] / 'naturezas.csv',
        'qualificacoes': DIRS['auxiliar'] / 'qualificacoes.csv', 
        'motivos': DIRS['auxiliar'] / 'motivos.csv',
        'paises': DIRS['auxiliar'] / 'paises.csv',
        'cnaes': DIRS['auxiliar'] / 'cnaes.csv',
        'municipios': DIRS['auxiliar'] / 'municipios.csv'
    }

def get_output_files():
    """Retorna dict com paths dos arquivos de saída"""
    return {
        'empresas': DIRS['database'] / 'empresas_final.csv',
        'estabelecimentos': DIRS['database'] / 'estabelecimentos_final.csv',
        'socios': DIRS['database'] / 'socios_final.csv'
    }

if __name__ == "__main__":
    # Teste das configurações
    print("Configurações do projeto CNAE:")
    print(f"Diretório raiz: {PROJECT_ROOT}")
    print("\nDiretórios:")
    for name, path in DIRS.items():
        print(f"  {name}: {path}")
    
    print(f"\nArquivos auxiliares:")
    for name, path in get_auxiliar_files().items():
        print(f"  {name}: {path}")
    
    print(f"\nArquivos de saída:")
    for name, path in get_output_files().items():
        print(f"  {name}: {path}")
