import pandas as pd
import os
from tqdm import tqdm
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
from src.schemas.empSchema import EMPRESAS_SCHEMA

def empresasConstructor(chunk_size=100000):
    """Processa e enriquece dados de empresas"""
    input_directory = Path("Data")
    csv_file = input_directory / "empresas_final.csv"
    natureza_path = Path("Auxiliar") / "naturezas.csv"
    output_path = Path("database") / "empresas_final.csv"
    
    if not csv_file.exists():
        print(f"❌ Arquivo não encontrado: {csv_file}")
        return
    
    try:
        porte = pd.DataFrame({
            "id_porte": ["00", "01", "03", "05"],
            "descricao_porte": ["NÃO INFORMADO", "MICRO EMPRESA", "EMPRESA DE PEQUENO PORTE", "DEMAIS"]
        })
        
        natureza = pd.read_csv(natureza_path, sep=';', encoding='utf-8', dtype=str)
        
        output_path.parent.mkdir(exist_ok=True)
        
        print("📊 Processando empresas...")
        
        chunk_list = []
        for chunk in tqdm(pd.read_csv(csv_file, sep=';', chunksize=chunk_size, dtype=str), 
                         desc="Processando chunks"):
            
            chunk = chunk.merge(porte, left_on='porte_empresa', right_on='id_porte', how='left')
            chunk = chunk.merge(natureza, left_on='natureza_juridica', right_on='codigo', how='left')
            chunk_list.append(chunk)
        
        df_final = pd.concat(chunk_list, ignore_index=True)
        df_final.to_csv(output_path, index=False, sep=';', encoding='utf-8')
        
        print(f"✅ Arquivo processado salvo: {output_path}")
        print(f"📊 Total de registros: {len(df_final):,}")
        
    except Exception as e:
        print(f"❌ Erro durante processamento: {e}")
        import traceback
        traceback.print_exc()
