import pandas as pd
import os
from tqdm import tqdm
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
from src.schemas.sociosSchema import SOCIOS_SCHEMA

def sociosConstructor(chunk_size=100000):
    """Processa e enriquece dados de sócios"""
    input_directory = Path("Data")
    csv_file = input_directory / "socios_final.csv"
    qualificacoes_path = Path("Auxiliar") / "qualificacoes.csv"
    paises_path = Path("Auxiliar") / "paises.csv"
    output_path = Path("database") / "socios_final.csv"
    
    if not csv_file.exists():
        print(f"❌ Arquivo não encontrado: {csv_file}")
        return
    
    try:
        qualificacoes = pd.read_csv(qualificacoes_path, sep=';', encoding='latin1', dtype=str)
        paises = pd.read_csv(paises_path, sep=';', encoding='utf-8', dtype=str)
        
        output_path.parent.mkdir(exist_ok=True)
        
        print("👥 Processando sócios...")
        
        chunk_list = []
        for chunk in tqdm(pd.read_csv(csv_file, sep=';', chunksize=chunk_size, dtype=str), 
                         desc="Processando chunks"):
            
            chunk = chunk.merge(qualificacoes, left_on='qualificacao_socio', right_on='id', how='left')
            chunk = chunk.merge(paises, left_on='pais', right_on='codigo', how='left')
            
            chunk_list.append(chunk)
        
        df_final = pd.concat(chunk_list, ignore_index=True)
        df_final.to_csv(output_path, index=False, sep=';', encoding='utf-8')
        
        print(f"✅ Arquivo processado salvo: {output_path}")
        print(f"📊 Total de registros: {len(df_final):,}")
        
    except Exception as e:
        print(f"❌ Erro durante processamento: {e}")
        import traceback
        traceback.print_exc()
