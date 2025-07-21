import pandas as pd
import os
from tqdm import tqdm
import glob

import pandas as pd
import os
from tqdm import tqdm
import glob
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from Schemas.sociosSchema import SOCIOS_SCHEMA

QUALIFICACOES_SCHEMA = ["id", "descricao"]
PAISES_SCHEMA = ["id_pais", "descricao_pais"]

def sociosConstructor(chunk_size=100000):
    input_directory = "./Data/"
    csv_pattern = os.path.join(input_directory, "socios*.csv")
    output_path = "./database/socios_final.csv"

    QUALIFICACOES_SCHEMA = ["id", "descricao"]
    PAISES_SCHEMA = ["id_pais", "descricao_pais"]

    csv_files = sorted(glob.glob(csv_pattern))
    if not csv_files:
        print(f"ERRO: Nenhum arquivo CSV encontrado com o padrão '{csv_pattern}'.")
        return

    if os.path.exists(output_path):
        os.remove(output_path)
        print("Arquivo existente removido.")

    try:
        identificador_socio = pd.DataFrame({
            "codigo": ["1", "2", "3"],
            "descricao_identificador": ["PESSOA JURÍDICA", "PESSOA FÍSICA", "ESTRANGEIRO"]
        })
        faixa_etaria = pd.DataFrame({
            "codigo_faixa": ["1", '2', "3", "4", "5", "6", "7", "8", "9", "0"],
            "descricao_faixa": [
                "0 a 12 anos",
                "13 a 20 anos",
                "21 a 30 anos",
                "31 a 40 anos",
                "41 a 50 anos",
                "51 a 60 anos",
                "61 a 70 anos",
                "71 a 80 anos",
                "maiores de 80 anos",
                "não se aplica"
            ]
        })
        qualificacoes = pd.read_csv(
            "./Auxiliar/qualificacoes.csv",
            sep=';',
            dtype=str,
            encoding='latin1',
            names=QUALIFICACOES_SCHEMA,
            header=None
        )
        pais = pd.read_csv(
            "./Auxiliar/paises.csv",
            sep=';',
            dtype=str,
            encoding='latin1',
            names=PAISES_SCHEMA,
            header=None
        )
    except FileNotFoundError as e:
        print(f"ERRO: Arquivo auxiliar não encontrado: {e}. Encerrando.")
        return

    total_linhas = 0
    primeiro_chunk = True

    for csv_file in tqdm(csv_files, desc="Processando arquivos CSV"):
        try:
            chunk_iter = pd.read_csv(
                csv_file,
                sep=';',
                header=None,
                names=SOCIOS_SCHEMA,
                dtype=str,
                encoding='latin1',
                on_bad_lines='warn',
                chunksize=chunk_size
            )
            
            for socios_chunk in chunk_iter:
                # Ajuste de tipos para merge
                if 'qualificacao_socio' in socios_chunk.columns:
                    socios_chunk['qualificacao_socio'] = socios_chunk['qualificacao_socio'].astype(str)
                if 'qualificacao_representante' in socios_chunk.columns:
                    socios_chunk['qualificacao_representante'] = socios_chunk['qualificacao_representante'].astype(str)
                if 'identificador_socio' in socios_chunk.columns:
                    socios_chunk['identificador_socio'] = socios_chunk['identificador_socio'].astype(str)
                if 'faixa_etaria' in socios_chunk.columns:
                    socios_chunk['faixa_etaria'] = socios_chunk['faixa_etaria'].astype(str)
                if 'pais' in socios_chunk.columns:
                    socios_chunk['pais'] = socios_chunk['pais'].astype(str)

                qualificacoes_socio = qualificacoes.rename(columns={'id': 'id_qs', 'descricao': 'descricao_qs'})
                qualificacoes_representante = qualificacoes.rename(columns={'id': 'id_qr', 'descricao': 'descricao_qr'})

                if 'qualificacao_socio' in socios_chunk.columns:
                    qualificacoes_socio['id_qs'] = qualificacoes_socio['id_qs'].astype(str)
                    socios_chunk = socios_chunk.merge(
                        qualificacoes_socio,
                        left_on='qualificacao_socio',
                        right_on='id_qs',
                        how='left'
                    )

                if 'qualificacao_representante' in socios_chunk.columns:
                    qualificacoes_representante['id_qr'] = qualificacoes_representante['id_qr'].astype(str)
                    socios_chunk = socios_chunk.merge(
                        qualificacoes_representante,
                        left_on='qualificacao_representante',
                        right_on='id_qr',
                        how='left'
                    )

                if 'identificador_socio' in socios_chunk.columns:
                    identificador_socio['codigo'] = identificador_socio['codigo'].astype(str)
                    socios_chunk = socios_chunk.merge(
                        identificador_socio,
                        left_on='identificador_socio',
                        right_on='codigo',
                        how='left'
                    )

                if 'faixa_etaria' in socios_chunk.columns:
                    faixa_etaria['codigo_faixa'] = faixa_etaria['codigo_faixa'].astype(str)
                    socios_chunk = socios_chunk.merge(
                        faixa_etaria,
                        left_on='faixa_etaria',
                        right_on='codigo_faixa',
                        how='left'
                    )

                if 'pais' in socios_chunk.columns:
                    pais['id_pais'] = pais['id_pais'].astype(str)
                    socios_chunk = socios_chunk.merge(
                        pais,
                        left_on='pais',
                        right_on='id_pais',
                        how='left'
                    )

                if 'descricao_qs' in socios_chunk.columns:
                    socios_chunk['qualificacao_socio'] = socios_chunk['descricao_qs']
                if 'descricao_qr' in socios_chunk.columns:
                    socios_chunk['qualificacao_representante'] = socios_chunk['descricao_qr']
                if 'descricao_identificador' in socios_chunk.columns:
                    socios_chunk['identificador_socio'] = socios_chunk['descricao_identificador']
                if 'descricao_pais' in socios_chunk.columns:
                    socios_chunk['pais'] = socios_chunk['descricao_pais']
                if 'descricao_faixa' in socios_chunk.columns:
                    socios_chunk['faixa_etaria'] = socios_chunk['descricao_faixa']

                cols_to_drop = [
                    'id_qs', 'descricao_qs',
                    'id_qr', 'descricao_qr',
                    'descricao_identificador',
                    'codigo_faixa',
                    'codigo',
                    'id_pais',
                    'descricao_pais',
                    'descricao_faixa'
                ]
                socios_chunk = socios_chunk.drop(columns=[col for col in cols_to_drop if col in socios_chunk.columns], errors='ignore')

                socios_chunk.to_csv(
                    output_path,
                    mode='a',
                    index=False,
                    sep=';',
                    header=primeiro_chunk
                )
                primeiro_chunk = False
                total_linhas += len(socios_chunk)
        except Exception as e:
            print(f"Aviso: Erro ao ler o arquivo '{csv_file}': {e}. Pulando este arquivo.")
            continue

    print(f"\nDados processados e exportados para '{output_path}'")
    print(f"Total de linhas processadas: {total_linhas}")

if __name__ == "__main__":
    sociosConstructor(chunk_size=100000)