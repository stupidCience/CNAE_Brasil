import pandas as pd
import os
from tqdm import tqdm
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
from src.schemas.estabSchema import ESTABELECIMENTOS_SCHEMA

def estabelecimentoConstructor(chunk_size=100000):
    """Processa e enriquece dados de estabelecimentos"""
    input_directory = Path("Data")
    csv_file = input_directory / "estabelecimentos_final.csv"
    municipios_path = Path("Auxiliar") / "municipios.csv"
    cnaes_path = Path("Auxiliar") / "cnaes.csv"
    paises_path = Path("Auxiliar") / "paises.csv"
    motivos_path = Path("Auxiliar") / "motivos.csv"
    output_path = Path("database") / "estabelecimentos_final.csv"
    
    if not csv_file.exists():
        print(f"❌ Arquivo não encontrado: {csv_file}")
        return
    
    try:
        municipios = pd.read_csv(municipios_path, sep=';', encoding='utf-8', dtype=str)
        cnaes = pd.read_csv(cnaes_path, sep=';', encoding='utf-8', dtype=str)
        paises = pd.read_csv(paises_path, sep=';', encoding='utf-8', dtype=str)
        motivos = pd.read_csv(motivos_path, sep=';', encoding='utf-8', dtype=str)
        
        output_path.parent.mkdir(exist_ok=True)
        
        print("🏢 Processando estabelecimentos...")
        
        chunk_list = []
        for chunk in tqdm(pd.read_csv(csv_file, sep=';', chunksize=chunk_size, dtype=str), 
                         desc="Processando chunks"):
            
            chunk['CNPJ'] = chunk['cnpj_basico'] + chunk['cnpj_ordem'] + chunk['cnpj_dv']
            
            chunk = chunk.merge(municipios, left_on='municipio', right_on='codigo', how='left')
            chunk = chunk.merge(cnaes, left_on='cnae_fiscal_principal', right_on='codigo', how='left')
            chunk = chunk.merge(paises, left_on='pais', right_on='codigo', how='left')
            chunk = chunk.merge(motivos, left_on='motivo_situacao_cadastral', right_on='codigo', how='left')
            
            chunk_list.append(chunk)
        
        df_final = pd.concat(chunk_list, ignore_index=True)
        df_final.to_csv(output_path, index=False, sep=';', encoding='utf-8')
        
        print(f"✅ Arquivo processado salvo: {output_path}")
        print(f"📊 Total de registros: {len(df_final):,}")
        
    except Exception as e:
        print(f"❌ Erro durante processamento: {e}")
        import traceback
        traceback.print_exc()
