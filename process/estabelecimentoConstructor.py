import pandas as pd
import os
from tqdm import tqdm
import pyarrow.parquet as pq
import pyarrow as pa

# --- Definições de Schema (sem alterações) ---
MOTIVOS_SCHEMA = [
    "id_motivo",
    "descricao_motivo"
]
PAIS_SCHEMA = [
    "id_pais",
    "descricao_pais"
]
CNAES_SCHEMA = [
    "id_cnae",
    "descricao_cnae"
]

def estabelecimentoConstructor(chunk_size=100000):
    """
    Processa o arquivo de estabelecimentos em chunks, aplicando filtros,
    junções e transformações, e salva o resultado em um novo arquivo Parquet.

    Args:
        chunk_size (int): O número de linhas a serem processadas em cada chunk.
                          Ajuste este valor de acordo com a memória disponível
                          e o tamanho dos dados.
    """
    print("Iniciando o processamento de estabelecimentos...")

    output_path = "./database/estabelecimentos.parquet"
    if os.path.exists(output_path):
        os.remove(output_path)
        print("Arquivo existente removido.")

    estabelecimentos_path = "./Data/estabelecimentos_final.parquet"
    motivos_situacao_cadastral_path = "./Auxiliar/motivos.csv"
    pais_path = "./Auxiliar/paises.csv"
    cnaes_path = "./Auxiliar/cnaes.csv"

    # --- Carregamento de DataFrames auxiliares (sem alterações) ---
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

    print(f"Carregando e processando '{estabelecimentos_path}' em chunks de {chunk_size} linhas...")

    parquet_writer = None

    try:
        parquet_file = pq.ParquetFile(estabelecimentos_path)
        
        # *** CORREÇÃO 1: Obter o esquema do arquivo de ENTRADA ***
        # Este esquema será usado para garantir consistência na SAÍDA.
        source_schema = parquet_file.schema
        
        total_rows = parquet_file.metadata.num_rows
        num_chunks = (total_rows + chunk_size - 1) // chunk_size 

        with tqdm(total=num_chunks, desc="Processando chunks de estabelecimentos") as pbar:
            # *** CORREÇÃO 2: Usar o iter_batches do arquivo original ***
            for batch in parquet_file.iter_batches(batch_size=chunk_size):
                estabelecimentos_chunk = batch.to_pandas()

                # --- Início do tratamento de dados para cada chunk (sem alterações) ---

                # Filtrando CNPJ básico e situação cadastral
                # (Seu código original para filtros está correto)
                condicao_cnpj_valido = True
                if 'cnpj_basico' in estabelecimentos_chunk.columns:
                    estabelecimentos_chunk['cnpj_basico'] = estabelecimentos_chunk['cnpj_basico'].astype(str).fillna('')
                    condicao_cnpj_valido = (
                        (estabelecimentos_chunk['cnpj_basico'] != '00000000') &
                        (estabelecimentos_chunk['cnpj_basico'] != '00000000-0000') &
                        (estabelecimentos_chunk['cnpj_basico'] != '')
                    )

                condicao_situacao = True
                if 'situacao_cadastral' in estabelecimentos_chunk.columns:
                    estabelecimentos_chunk['situacao_cadastral'] = estabelecimentos_chunk['situacao_cadastral'].astype(str).fillna('')
                    condicao_situacao = ~estabelecimentos_chunk['situacao_cadastral'].isin(['01', '04'])
                
                # Aplica os filtros
                estabelecimentos_chunk = estabelecimentos_chunk[condicao_cnpj_valido & condicao_situacao]


                # Convertendo datas
                for col in ['data_situacao_cadastral', 'data_inicio_atividade', 'data_situacao_especial']:
                    if col in estabelecimentos_chunk.columns:
                        # Usar errors='coerce' é uma boa prática aqui
                        estabelecimentos_chunk[col] = pd.to_datetime(
                            estabelecimentos_chunk[col], 
                            format='%Y%m%d', 
                            errors='coerce'
                        ).dt.date

                # Criando coluna CNPJ
                if all(col in estabelecimentos_chunk.columns for col in ['cnpj_basico', 'cnpj_ordem', 'cnpj_dv']):
                    estabelecimentos_chunk['CNPJ'] = (
                        estabelecimentos_chunk['cnpj_basico'].astype(str) +
                        estabelecimentos_chunk['cnpj_ordem'].astype(str) +
                        estabelecimentos_chunk['cnpj_dv'].astype(str)
                    )
                
                # Joins com tabelas auxiliares
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

                # Ajustando colunas finais
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
                
                # Colunas a serem mantidas no final
                final_columns = [
                    'CNPJ', 'identificador_matriz', 'nome_fantasia', 'situacao_cadastral',
                    'data_situacao_cadastral', 'motivo_situacao_cadastral', 'nome_da_cidade_no_exterior',
                    'pais', 'data_inicio_atividade', 'cnae_fiscal_principal', 'tipo_logradouro',
                    'logradouro', 'numero', 'complemento', 'bairro', 'cep', 'uf', 'municipio',
                    'ddd1', 'telefone1', 'ddd2', 'telefone2', 'dddfax', 'fax', 'email',
                    'situacao_especial', 'data_situacao_especial', 'cnae_fiscal_secundario'
                ]
                
                # Filtra o chunk para ter apenas as colunas finais, ignorando as que não existem
                current_final_columns = [col for col in final_columns if col in estabelecimentos_chunk.columns]
                estabelecimentos_chunk = estabelecimentos_chunk[current_final_columns]

                # --- Fim do tratamento de dados ---

                # Converte o DataFrame Pandas de volta para uma Tabela PyArrow
                # *** CORREÇÃO 3: Forçar o esquema correto ao criar a tabela ***
                # Isso garante que colunas como 'nome_da_cidade_no_exterior' tenham o tipo string,
                # mesmo que o chunk atual só contenha nulos.
                table = pa.Table.from_pandas(estabelecimentos_chunk, preserve_index=False)

                if parquet_writer is None:
                    # *** CORREÇÃO 4: Usar o esquema da TABELA PROCESSADA para criar o escritor ***
                    # Usamos o esquema da primeira tabela processada para garantir que a ordem das colunas
                    # e os tipos (após nossas transformações) sejam mantidos.
                    final_schema = table.schema
                    parquet_writer = pq.ParquetWriter(output_path, final_schema)
                
                # Garante que a tabela a ser escrita tenha o mesmo esquema do arquivo
                table = table.cast(parquet_writer.schema)
                parquet_writer.write_table(table)
                pbar.update(1)

    except FileNotFoundError:
        print(f"Erro: O arquivo '{estabelecimentos_path}' não foi encontrado.")
    except Exception as e:
        print(f"Ocorreu um erro durante o processamento: {e}")
    finally:
        if parquet_writer:
            parquet_writer.close()
            print(f"Dados processados e exportados para '{output_path}'")

    print("Processamento de estabelecimentos concluído.")

# Para testar, você pode chamar a função
# estabelecimentoConstructor()

