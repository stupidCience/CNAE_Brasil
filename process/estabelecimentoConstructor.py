import pandas as pd
import os
from tqdm import tqdm
import glob
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from Schemas.estabSchema import ESTABELECIMENTOS_SCHEMA

def estabelecimentoConstructor(chunk_size=100000):
    input_directory = "./Data/"
    csv_pattern = os.path.join(input_directory, "estabelecimentos*.csv")
    motivos_situacao_cadastral_path = "./Auxiliar/motivos.csv"
    pais_path = "./Auxiliar/paises.csv"
    cnaes_path = "./Auxiliar/cnaes.csv"
    output_path = "./database/estabelecimentos_final.csv"

    MOTIVOS_SCHEMA = ["id_motivo", "descricao_motivo"]
    PAIS_SCHEMA = ["id_pais", "descricao_pais"]
    CNAES_SCHEMA = ["id_cnae", "descricao_cnae"]

    csv_files = sorted(glob.glob(csv_pattern))
    if not csv_files:
        print(f"ERRO: Nenhum arquivo CSV encontrado com o padrão '{csv_pattern}'.")
        return

    try:
        motivos_situacao_cadastral = pd.read_csv(motivos_situacao_cadastral_path, sep=';', dtype=str, encoding='utf-8', names=MOTIVOS_SCHEMA, header=None)
        identificador_matriz = pd.DataFrame({
            "codigo_matriz": ["1", "2"],
            "descricao_matriz": ["MATRIZ", "FILIAL"]
        })
        situacao_cadastral = pd.DataFrame({
            "codigo_situacao": ["01", "2", "3", "4", "08"],
            "descricao_situacao_cadastral": ["NULA", "ATIVA", "SUSPENSA", "INAPTA", "BAIXADA"]
        })
        pais = pd.read_csv(pais_path, sep=';', dtype=str, encoding='latin1', names=PAIS_SCHEMA, header=None)
        cnaes = pd.read_csv(cnaes_path, sep=';', dtype=str, encoding='latin1', names=CNAES_SCHEMA, header=None)
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
                header=None,
                names=ESTABELECIMENTOS_SCHEMA,
                dtype=str,
                encoding='latin1',
                on_bad_lines='warn',
                chunksize=chunk_size
            )
            for estabelecimentos_chunk in chunk_iter:
                if 'cnpj_basico' in estabelecimentos_chunk.columns:
                    estabelecimentos_chunk['cnpj_basico'] = estabelecimentos_chunk['cnpj_basico'].astype(str).fillna('')
                    condicao_cnpj_valido = (
                        (estabelecimentos_chunk['cnpj_basico'] != '00000000') &
                        (estabelecimentos_chunk['cnpj_basico'] != '00000000-0000') &
                        (estabelecimentos_chunk['cnpj_basico'] != '')
                    )
                    estabelecimentos_chunk = estabelecimentos_chunk[condicao_cnpj_valido]


                for col in ['data_situacao_cadastral', 'data_inicio_atividade', 'data_situacao_especial']:
                    if col in estabelecimentos_chunk.columns:
                        estabelecimentos_chunk[col] = pd.to_datetime(
                            estabelecimentos_chunk[col], 
                            format='%Y%m%d', 
                            errors='coerce'
                        ).dt.date

                if all(col in estabelecimentos_chunk.columns for col in ['cnpj_basico', 'cnpj_ordem', 'cnpj_dv']):
                    estabelecimentos_chunk['CNPJ'] = (
                        estabelecimentos_chunk['cnpj_basico'].astype(str) +
                        estabelecimentos_chunk['cnpj_ordem'].astype(str) +
                        estabelecimentos_chunk['cnpj_dv'].astype(str)
                    )

                if 'identificador_matriz' in estabelecimentos_chunk.columns:
                    estabelecimentos_chunk = estabelecimentos_chunk.merge(identificador_matriz, left_on='identificador_matriz', right_on='codigo_matriz', how='left')
                if 'situacao_cadastral' in estabelecimentos_chunk.columns:
                    estabelecimentos_chunk = estabelecimentos_chunk.merge(situacao_cadastral, left_on='situacao_cadastral', right_on='codigo_situacao', how='left')
                if 'motivo_situacao_cadastral' in estabelecimentos_chunk.columns:
                    estabelecimentos_chunk = estabelecimentos_chunk.merge(motivos_situacao_cadastral, left_on='motivo_situacao_cadastral', right_on='id_motivo', how='left')
                if 'pais' in estabelecimentos_chunk.columns:
                    estabelecimentos_chunk = estabelecimentos_chunk.merge(pais, left_on='pais', right_on='id_pais', how='left')
                if 'cnae_fiscal_principal' in estabelecimentos_chunk.columns:
                    estabelecimentos_chunk = estabelecimentos_chunk.merge(cnaes, left_on='cnae_fiscal_principal', right_on='id_cnae', how='left')

                if 'descricao_motivo' in estabelecimentos_chunk.columns:
                    estabelecimentos_chunk['motivo_situacao_cadastral'] = estabelecimentos_chunk['descricao_motivo']
                if 'descricao_matriz' in estabelecimentos_chunk.columns:
                    estabelecimentos_chunk['identificador_matriz'] = estabelecimentos_chunk['descricao_matriz']
                if 'descricao_situacao_cadastral' in estabelecimentos_chunk.columns:
                    estabelecimentos_chunk['situacao_cadastral'] = estabelecimentos_chunk['descricao_situacao_cadastral']
                if 'descricao_pais' in estabelecimentos_chunk.columns:
                    estabelecimentos_chunk['pais'] = estabelecimentos_chunk['descricao_pais']
                if 'descricao_cnae' in estabelecimentos_chunk.columns:
                    estabelecimentos_chunk['cnae_fiscal_principal'] = estabelecimentos_chunk['descricao_cnae']

                final_columns = [
                    'CNPJ', 'identificador_matriz', 'nome_fantasia', 'situacao_cadastral',
                    'data_situacao_cadastral', 'motivo_situacao_cadastral', 'nome_da_cidade_no_exterior',
                    'pais', 'data_inicio_atividade', 'cnae_fiscal_principal', 'tipo_logradouro',
                    'logradouro', 'numero', 'complemento', 'bairro', 'cep', 'uf', 'municipio',
                    'ddd1', 'telefone1', 'ddd2', 'telefone2', 'dddfax', 'fax', 'email',
                    'situacao_especial', 'data_situacao_especial', 'cnae_fiscal_secundario'
                ]
                current_final_columns = [col for col in final_columns if col in estabelecimentos_chunk.columns]
                estabelecimentos_chunk = estabelecimentos_chunk[current_final_columns]

                estabelecimentos_chunk.to_csv(
                    output_path,
                    mode='a',
                    index=False,
                    sep=';',
                    header=primeiro_chunk
                )
                primeiro_chunk = False
                total_linhas += len(estabelecimentos_chunk)
        except Exception as e:
            print(f"Aviso: Erro ao ler o arquivo '{csv_file}': {e}. Pulando este arquivo.")
            continue

    print(f"\nDados processados e exportados para '{output_path}'")
    print(f"Total de linhas processadas: {total_linhas}")

if __name__ == "__main__":
    estabelecimentoConstructor(chunk_size=100000)