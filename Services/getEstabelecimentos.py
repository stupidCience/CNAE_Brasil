import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pathlib import Path
import sys
import zipfile
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from tqdm import tqdm

from Schemas.estabSchema import ESTABELECIMENTOS_SCHEMA as COLUMNS

def extrair_e_limpar(diretorio: Path):
    zips = list(diretorio.glob("*.zip"))
    contador_csv = 1

    for zip_path in zips:
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                for file_info in zip_ref.infolist():
                    extracted_path = diretorio / file_info.filename
                    zip_ref.extract(file_info, path=diretorio)

                    if file_info.filename.upper().endswith(".ESTABELE"):
                        new_name = Path(file_info.filename).with_suffix(".csv")
                        new_path = diretorio / new_name
                        extracted_path.rename(new_path)

                        sequencial_name = f"estabelecimentos{contador_csv:02d}.csv"
                        final_path = diretorio / sequencial_name

                        if final_path.exists():
                            final_path.unlink()

                        new_path.rename(final_path)
                        print(f"Renomeado: {file_info.filename} → {final_path.name}")

                        contador_csv += 1
                    else:
                        print(f"Extraído: {file_info.filename}")
            
            zip_path.unlink()
            print(f"Removido: {zip_path.name}")
        except zipfile.BadZipFile:
            print(f"Arquivo corrompido ou inválido: {zip_path.name}")
        except Exception as e:
            print(f"Erro ao processar {zip_path.name}: {e}")

def aplicar_schema_estabelecimentos(diretorio: Path, colunas: list[str], chunk_size_csv=100_000):
    """
    Lê arquivos CSV em chunks, aplica o schema e salva os chunks diretamente
    em um arquivo Parquet final, evitando carregar tudo na memória.
    Cada arquivo CSV é removido após seu processamento completo.

    Args:
        diretorio (Path): O diretório onde os arquivos CSV estão localizados.
        colunas (list[str]): A lista de nomes de colunas a serem aplicadas.
        chunk_size_csv (int): O número de linhas a serem lidas de cada CSV em um chunk.
    """
    csv_files = sorted(diretorio.glob("estabelecimentos*.csv"))
    final_parquet_path = diretorio / "estabelecimentos_final.parquet"

    if final_parquet_path.exists():
        final_parquet_path.unlink()
        print(f"Arquivo existente removido: {final_parquet_path.name}")

    parquet_writer = None # Inicializa o escritor Parquet

    if not csv_files:
        print("Nenhum arquivo CSV 'estabelecimentos*.csv' encontrado para processar.")
        return

    # --- NOVO: Define o esquema PyArrow explicitamente ---
    # Assume que todas as colunas podem ser strings e são anuláveis dos CSVs brutos
    parquet_schema_fields = []
    for col_name in colunas:
        parquet_schema_fields.append(pa.field(col_name, pa.string(), nullable=True))
    explicit_parquet_schema = pa.schema(parquet_schema_fields)

    with tqdm(total=len(csv_files), desc="Processando arquivos CSV de estabelecimentos") as pbar_csv:
        for i, csv_file in enumerate(csv_files):
            print(f"Iniciando processamento de {csv_file.name}...") 
            try:
                chunk_iter = pd.read_csv(
                    csv_file,
                    sep=';',
                    header=None,
                    dtype=str,
                    encoding='latin1',
                    chunksize=chunk_size_csv
                )
                
                for chunk_num, chunk in enumerate(chunk_iter):
                    print(f"  Processando chunk {chunk_num} de {csv_file.name}. Shape: {chunk.shape}, Empty: {chunk.empty}")
                    if chunk.empty:
                        continue # Pula chunks vazios

                    chunk.columns = colunas
                    
                    if parquet_writer is None:
                        # Cria o escritor Parquet com o ESQUEMA EXPLÍCITO
                        parquet_writer = pq.ParquetWriter(final_parquet_path, explicit_parquet_schema)
                        print(f"  ParquetWriter inicializado para {final_parquet_path.name} com esquema explícito.") 
                    
                    # Converte o chunk para PyArrow Table usando o esquema explícito
                    # Isso irá validar e coagir os tipos para corresponder ao esquema definido
                    table_chunk = pa.Table.from_pandas(chunk, schema=explicit_parquet_schema, preserve_index=False)
                    parquet_writer.write_table(table_chunk)
                
                # --- Removido o arquivo CSV logo após seu processamento completo ---
                try:
                    csv_file.unlink()
                    print(f"Removido arquivo intermediário: {csv_file.name}")
                except Exception as e:
                    print(f"Erro ao remover {csv_file.name}: {e}")

                print(f"Processamento de {csv_file.name} concluído.") 
                pbar_csv.update(1)
                
            except pd.errors.EmptyDataError:
                print(f"Atenção: O arquivo CSV '{csv_file.name}' está vazio ou contém apenas o cabeçalho. Pulando.")
                try:
                    csv_file.unlink()
                    print(f"Removido arquivo intermediário vazio: {csv_file.name}")
                except Exception as e:
                    print(f"Erro ao remover arquivo CSV vazio {csv_file.name}: {e}")
                pbar_csv.update(1)
            except Exception as e:
                print(f"Erro inesperado ao processar {csv_file.name}: {e}")
                if parquet_writer:
                    parquet_writer.close() 
                # Não está quebrando para tentar processar outros arquivos CSV
                # break 

    if parquet_writer:
        parquet_writer.close() 
        print(f"Dados combinados salvos em: {final_parquet_path.name}")
    else:
        print("Nenhum dado foi processado e escrito para o arquivo Parquet final.")

def getEstab():
    today = datetime.today()
    previousMonth = today - relativedelta(months=1)
    filename = previousMonth.strftime('%Y-%m')

    root_dir = Path(__file__).resolve().parent.parent
    output_dir = root_dir / "Data"
    output_dir.mkdir(parents=True, exist_ok=True)

    print("Tentando baixar bases de Estabelecimentos...")
    contador = 0
    while True:
        try:
            base_url = f"https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/{filename}/Estabelecimentos{contador}.zip"
            zip_path = output_dir / f"estabelecimentos_{filename}_{contador}.zip"

            with requests.get(base_url, stream=True) as r:
                if r.status_code == 404:
                    print(f"Nenhum arquivo encontrado em: {base_url}")
                    break
                r.raise_for_status()
                total_length = int(r.headers.get('Content-Length', 0))
                downloaded = 0

                with open(zip_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)
                            done = int(50 * downloaded / total_length) if total_length else 0
                            percent = (downloaded / total_length) * 100 if total_length else 0
                            sys.stdout.write(f"\r[{'=' * done}{' ' * (50 - done)}] {percent:.2f}%")
                            sys.stdout.flush()
                print(f"\nArquivo baixado para: {zip_path.name}")
                contador += 1

        except requests.exceptions.RequestException as e:
            print(f"Erro ao baixar arquivo: {e}")
            break

    extrair_e_limpar(output_dir)
    aplicar_schema_estabelecimentos(output_dir, COLUMNS, chunk_size_csv=100_000) 
    print("Processamento concluído.")