"""
Script principal para execução completa do processo ETL CNAE
Este script orquestra o download, processamento e validação dos dados
"""

import sys
from pathlib import Path

# Adiciona o diretório raiz ao path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from Services.getEmpresas import getEmp
from Services.getEstabelecimentos import getEstab  
from Services.getSocios import getSocios
from process.empresasConstructor import empresasConstructor
from process.estabelecimentoConstructor import estabelecimentoConstructor
from process.sociosConstructor import sociosConstructor

def run_full_etl():
    """Executa o processo ETL completo"""
    print("🚀 Iniciando processo ETL completo para dados CNAE...")
    
    try:
        # Fase 1: Download e Extração
        print("\n📥 FASE 1: Download e Extração de Dados")
        print("=" * 50)
        
        print("📊 Baixando dados de Empresas...")
        getEmp()
        
        print("🏢 Baixando dados de Estabelecimentos...")
        getEstab()
        
        print("👥 Baixando dados de Sócios...")
        getSocios()
        
        # Fase 2: Processamento e Enriquecimento
        print("\n⚙️  FASE 2: Processamento e Enriquecimento")
        print("=" * 50)
        
        print("📊 Processando dados de Empresas...")
        empresasConstructor()
        
        print("🏢 Processando dados de Estabelecimentos...")
        estabelecimentoConstructor()
        
        print("👥 Processando dados de Sócios...")
        sociosConstructor()
        
        # Fase 3: Conversão para Parquet
        print("\n🚀 FASE 3: Otimização - Convertendo para Parquet")
        print("=" * 50)
        from optimize_data import convert_to_parquet, benchmark_queries
        convert_to_parquet()
        benchmark_queries()
        
        print("\n🎉 Processo ETL concluído com sucesso!")
        print("📁 Arquivos CSV disponíveis em: ./database/")
        print("📁 Arquivos Parquet otimizados em: ./database/")
            
    except Exception as e:
        print(f"\n❌ Erro durante o processo ETL: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True

def run_download_only():
    """Executa apenas o download dos dados"""
    print("📥 Executando apenas download de dados...")
    getEmp()
    getEstab() 
    getSocios()

def run_processing_only():
    """Executa apenas o processamento dos dados"""
    print("⚙️ Executando apenas processamento de dados...")
    empresasConstructor()
    estabelecimentoConstructor()
    sociosConstructor()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Sistema ETL para dados CNAE')
    parser.add_argument('--mode', choices=['full', 'download', 'process'], 
                       default='full', help='Modo de execução')
    
    args = parser.parse_args()
    
    if args.mode == 'full':
        run_full_etl()
    elif args.mode == 'download':
        run_download_only()
    elif args.mode == 'process':
        run_processing_only()
