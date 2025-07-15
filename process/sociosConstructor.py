import pandas as pd
import os
from tqdm import tqdm
import pyarrow.parquet as pq
import pyarrow as pa
from tqdm import tqdm

SOCIOS_SCHEMA = [
    "id",
    "descricao"
]
PAISES_SCHEMA = ["id_pais", "descricao_pais"]

def sociosConstructor(chunk_size=100000):
    """
    Processa o arquivo de sócios em chunks, aplicando filtros, junções
    e transformações, e salva o resultado em um novo arquivo Parquet.

    Args:
        chunk_size (int): O número de linhas a serem processadas em cada chunk.
                          Ajuste este valor de acordo com a memória disponível
                          e o tamanho dos dados.
    """
    print("Iniciando o processamento de sócios...")

    output_path = "./database/socios.parquet"
    if os.path.exists(output_path):
        os.remove(output_path)
        print("Arquivo existente removido.")

    socios_path = "./Data/socios_final.parquet"

    # Carregar tabelas auxiliares UMA VEZ
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
        names=SOCIOS_SCHEMA,
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

    print(f"Carregando e processando '{socios_path}' em chunks de {chunk_size} linhas...")

    parquet_writer = None # Inicializa o escritor Parquet

    try:
        # Abre o arquivo Parquet de entrada
        parquet_file = pq.ParquetFile(socios_path)
        total_rows = parquet_file.metadata.num_rows
        # Calcula o número total de chunks para a barra de progresso
        num_chunks = (total_rows + chunk_size - 1) // chunk_size 

        # Itera sobre o arquivo Parquet em lotes (chunks)
        with tqdm(total=num_chunks, desc="Processando chunks de sócios") as pbar:
            for i, batch in enumerate(parquet_file.iter_batches(batch_size=chunk_size)):
                socios_chunk = batch.to_pandas() # Converte o lote PyArrow para DataFrame Pandas

                # --- Início do tratamento de dados para cada chunk ---
                
                # Aplica o filtro 'qualificacao_representante' != '00' a cada chunk
                if 'qualificacao_representante' in socios_chunk.columns:
                    socios_chunk = socios_chunk[socios_chunk['qualificacao_representante'] != '00']

                # Convertendo datas
                if 'data_entrada_sociedade' in socios_chunk.columns:
                    socios_chunk['data_entrada_sociedade'] = pd.to_datetime(socios_chunk['data_entrada_sociedade'], errors='coerce').dt.date
                
                # Join com qualificações de sócios e representante
                # Renomeia qualificacoes para evitar conflitos nas colunas 'id' e 'descricao'
                qualificacoes_socio = qualificacoes.rename(columns={'id': 'id_qs', 'descricao': 'descricao_qs'})
                qualificacoes_representante = qualificacoes.rename(columns={'id': 'id_qr', 'descricao': 'descricao_qr'})

                if 'qualificacao_socio' in socios_chunk.columns:
                    socios_chunk = socios_chunk.merge(
                        qualificacoes_socio,
                        left_on='qualificacao_socio',
                        right_on='id_qs',
                        how='left'
                    )

                if 'qualificacao_representante' in socios_chunk.columns:
                    socios_chunk = socios_chunk.merge(
                        qualificacoes_representante,
                        left_on='qualificacao_representante',
                        right_on='id_qr',
                        how='left'
                    )
                
                # Join com identificador de sócio
                if 'identificador_socio' in socios_chunk.columns:
                    socios_chunk = socios_chunk.merge(
                        identificador_socio,
                        left_on='identificador_socio',
                        right_on='codigo',
                        how='left'
                    )

                # Join com faixa etária
                if 'faixa_etaria' in socios_chunk.columns:
                    socios_chunk = socios_chunk.merge(
                        faixa_etaria,
                        left_on='faixa_etaria',
                        right_on='codigo_faixa',
                        how='left'
                    )

                # Join com país
                if 'pais' in socios_chunk.columns:
                    socios_chunk = socios_chunk.merge(
                        pais,
                        left_on='pais',
                        right_on='id_pais',
                        how='left'
                    )

                # Ajustando colunas finais
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

                # Removendo colunas auxiliares
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
                
                # --- Fim do tratamento de dados para cada chunk ---

                # Escreve o chunk processado no arquivo Parquet de saída
                if parquet_writer is None:
                    # Cria o escritor Parquet com o schema do primeiro chunk
                    schema = pa.Table.from_pandas(socios_chunk).schema
                    parquet_writer = pq.ParquetWriter(output_path, schema)
                
                parquet_writer.write_table(pa.Table.from_pandas(socios_chunk))
                pbar.update(1) # Atualiza a barra de progresso

        # Garante que o escritor Parquet seja fechado após o loop
        if parquet_writer:
            parquet_writer.close()

    except FileNotFoundError:
        print(f"Erro: O arquivo '{socios_path}' não foi encontrado. Certifique-se de que os dados foram baixados e processados inicialmente.")
    except Exception as e:
        print(f"Ocorreu um erro durante o processamento: {e}")
        # Tenta fechar o escritor Parquet mesmo em caso de erro
        if parquet_writer:
            parquet_writer.close() 

    print(f"Dados processados e exportados para '{output_path}'")
    print("Processamento de sócios concluído.")
    