import pandas as pd
import os
from tqdm import tqdm
import glob
import traceback
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def empresasConstructor(chunk_size=100000):
    input_directory = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "Data")
    input_directory = os.path.abspath(input_directory)
    
    csv_file = os.path.join(input_directory, "empresas_final.csv")
    natureza_path = "./CNAE_Brasil/Auxiliar/naturezas.csv"
    output_path = "./CNAE_Brasil/database/empresas_final.csv"
    
    print("Diretório atual:", os.getcwd())
    print("Arquivo procurado:", os.path.abspath(csv_file))
    
    if not os.path.exists(csv_file):
        print(f"ERRO: Nenhum arquivo CSV encontrado com o padrão '{csv_file}'.")
        return
    csv_files = [csv_file]
    
    try:
        porte = pd.DataFrame({
            "id_porte": ["00", "01", "03", "05"],
            "descricao_porte": ["NÃO INFORMADO", "MICRO EMPRESA", "EMPRESA DE PEQUENO PORTE", "DEMAIS"]
        })
        natureza = pd.read_csv(
            natureza_path,
            sep=';',
            dtype=str,
            encoding='latin1',
            names=["id_natureza", "descricao_natureza"],
            header=None
        )
    except FileNotFoundError as e:
        print(f"ERRO: Arquivo auxiliar não encontrado: {e}. Encerrando.")
        return

    if os.path.exists(output_path):
        os.remove(output_path)
        print(f"Arquivo CSV existente '{output_path}' removido.")

    total_linhas = 0
    primeiro_chunk = True

    for csv_file in tqdm(csv_files, desc="Processando arquivos CSV"):
        try:
            chunk_iter = pd.read_csv(
                csv_file,
                sep=';',
                encoding='latin1',
                chunksize=chunk_size
            )
            for empresas_chunk in chunk_iter:
                empresas_chunk = empresas_chunk[empresas_chunk['cnpj_basico'] != '00000000']

                # Ajuste de tipos para merge
                empresas_chunk['natureza_juridica'] = empresas_chunk['natureza_juridica'].astype(str)
                natureza['id_natureza'] = natureza['id_natureza'].astype(str)
                empresas_chunk['porte_empresa'] = empresas_chunk['porte_empresa'].astype(str)
                porte['id_porte'] = porte['id_porte'].astype(str)

                empresas_chunk = empresas_chunk.merge(natureza, left_on='natureza_juridica', right_on='id_natureza', how='left')
                empresas_chunk = empresas_chunk.merge(porte, left_on='porte_empresa', right_on='id_porte', how='left')
                empresas_chunk['natureza_juridica'] = empresas_chunk['descricao_natureza']
                empresas_chunk['porte_empresa'] = empresas_chunk['descricao_porte']

                final_columns = [
                    'cnpj_basico',
                    'razao_social',
                    'natureza_juridica',
                    'qualificacao_responsavel',
                    'capital_social',
                    'porte_empresa',
                    'ente_federativo_responsavel'
                ]
                for col in final_columns:
                    if col not in empresas_chunk.columns:
                        empresas_chunk[col] = pd.NA

                empresas_chunk = empresas_chunk[final_columns]
                empresas_chunk.to_csv(
                    output_path,
                    mode='a',
                    index=False,
                    sep=';',
                    header=primeiro_chunk
                )
                primeiro_chunk = False
                total_linhas += len(empresas_chunk)
        except Exception as e:
            print(f"Aviso: Erro ao ler o arquivo '{csv_file}': {e}. Pulando este arquivo.")
            traceback.print_exc()
            continue

    print(f"\nDados processados e exportados para '{output_path}'")
    print(f"Total de linhas processadas: {total_linhas}")

if __name__ == "__main__":
    empresasConstructor(chunk_size=100000)