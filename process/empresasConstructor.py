import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
from tqdm import tqdm
import glob
import traceback

def processar_empresas_completo(chunk_size=100000):
    """
    Script completo e autossuficiente para processar dados de empresas.
    Fase 1: Combina todos os arquivos CSV de entrada em um único DataFrame.
    Fase 2: Processa o DataFrame combinado em chunks, aplica transformações
             e salva em um arquivo Parquet final com esquema pré-definido.
    """
    print("Iniciando o processo completo de ponta a ponta...")

    # --- Configuração de Caminhos ---
    input_directory = "./Data/"
    csv_pattern = os.path.join(input_directory, "empresas*.csv")
    natureza_path = "./Auxiliar/naturezas.csv"
    output_path = "./database/empresas.parquet"

    # --- Esquema dos dados de ENTRADA (CSVs) ---
    # Define a estrutura esperada dos arquivos CSV brutos
    csv_schema = {
        'cnpj_basico': str,
        'razao_social': str,
        'natureza_juridica': str,
        'qualificacao_responsavel': str,
        'capital_social': str,
        'porte_empresa': str,
        'ente_federativo_responsavel': str
    }
    
    # --- Esquema de SAÍDA (Parquet) ---
    # Define a estrutura final e correta do arquivo Parquet
    final_schema_parquet = pa.schema([
        pa.field('cnpj_basico', pa.string()),
        pa.field('razao_social', pa.string()),
        pa.field('natureza_juridica', pa.string()),
        pa.field('qualificacao_responsavel', pa.string()),
        pa.field('capital_social', pa.string()),
        pa.field('porte_empresa', pa.string()),
        pa.field('ente_federativo_responsavel', pa.string())
    ])

    # =========================================================================
    # FASE 1: COMBINAR TODOS OS ARQUIVOS CSV EM MEMÓRIA
    # =========================================================================
    print("\n--- FASE 1: Combinando arquivos CSV ---")
    
    csv_files = sorted(glob.glob(csv_pattern))
    if not csv_files:
        print(f"ERRO: Nenhum arquivo CSV encontrado com o padrão '{csv_pattern}'.")
        return

    all_chunks = []
    print(f"Encontrados {len(csv_files)} arquivos para combinar.")
    for csv_file in tqdm(csv_files, desc="Lendo arquivos CSV"):
        try:
            chunk_df = pd.read_csv(
                csv_file,
                sep=';',
                header=None,
                names=list(csv_schema.keys()),
                dtype=csv_schema,
                encoding='latin1',
                on_bad_lines='warn'
            )
            all_chunks.append(chunk_df)
        except Exception as e:
            print(f"Aviso: Erro ao ler o arquivo '{csv_file}': {e}. Pulando este arquivo.")
            continue
    
    if not all_chunks:
        print("ERRO: Nenhum dado foi lido dos arquivos CSV. Encerrando.")
        return

    # Concatena todos os dataframes lidos em um único dataframe grande
    empresas_df_completo = pd.concat(all_chunks, ignore_index=True)
    print(f"FASE 1 concluída. Total de {len(empresas_df_completo)} registros combinados.")

    # =========================================================================
    # FASE 2: PROCESSAR O DATAFRAME COMBINADO E GERAR O PARQUET
    # =========================================================================
    print("\n--- FASE 2: Processando dados e gerando arquivo Parquet ---")
    
    # --- Carregamento de DataFrames auxiliares ---
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

    # Remove o arquivo de saída se ele já existir
    if os.path.exists(output_path):
        os.remove(output_path)
        print(f"Arquivo Parquet existente '{output_path}' removido.")

    parquet_writer = None
    try:
        # Inicializa o escritor Parquet com o esquema final definido
        parquet_writer = pq.ParquetWriter(output_path, final_schema_parquet)
        
        total_rows = len(empresas_df_completo)
        num_chunks = (total_rows + chunk_size - 1) // chunk_size

        with tqdm(total=total_rows, desc="Processando e gravando chunks") as pbar:
            for i in range(0, total_rows, chunk_size):
                # Pega um "pedaço" do dataframe completo
                empresas_chunk = empresas_df_completo.iloc[i:i + chunk_size]

                # --- Início do tratamento de dados para cada chunk ---
                condicao_cnpj_valido = (empresas_chunk['cnpj_basico'] != '00000000')
                empresas_chunk = empresas_chunk[condicao_cnpj_valido]

                # Join com Natureza Jurídica e Porte
                empresas_chunk = empresas_chunk.merge(natureza, left_on='natureza_juridica', right_on='id_natureza', how='left')
                empresas_chunk = empresas_chunk.merge(porte, left_on='porte_empresa', right_on='id_porte', how='left')

                # Substitui os códigos pelas descrições
                empresas_chunk['natureza_juridica'] = empresas_chunk['descricao_natureza']
                empresas_chunk['porte_empresa'] = empresas_chunk['descricao_porte']
                
                # Garante que todas as colunas do esquema final existam
                for field in final_schema_parquet:
                    if field.name not in empresas_chunk.columns:
                        empresas_chunk[field.name] = pd.NA
                
                # Reordena e seleciona as colunas para corresponder ao esquema final
                empresas_chunk = empresas_chunk[final_schema_parquet.names]
                
                # Converte para Tabela Arrow, forçando o esquema correto
                table = pa.Table.from_pandas(empresas_chunk, schema=final_schema_parquet, preserve_index=False)
                
                # Escreve a tabela no arquivo Parquet
                parquet_writer.write_table(table)
                pbar.update(len(empresas_chunk))

    except Exception as e:
        print(f"\nOcorreu um erro durante o processamento da FASE 2: {e}")
        traceback.print_exc()
    finally:
        if parquet_writer:
            parquet_writer.close()
            print(f"\nFASE 2 concluída. Dados processados e exportados para '{output_path}'")

    print("\nProcesso completo finalizado.")

# --- Para executar o processo ---
if __name__ == '__main__':
    processar_empresas_completo()
