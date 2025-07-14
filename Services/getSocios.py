import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pathlib import Path
import sys
import zipfile
import pandas as pd

from Schemas.sociosSchema import SOCIOS_SCHEMA as COLUMNS

def extrair_e_limpar(diretorio: Path):
    zips = list(diretorio.glob("*.zip"))
    contador_csv = 1

    for zip_path in zips:
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                for file_info in zip_ref.infolist():
                    extracted_path = diretorio / file_info.filename
                    zip_ref.extract(file_info, path=diretorio)

                    if file_info.filename.upper().endswith(".SOCIOCSV"):
                        new_name = Path(file_info.filename).with_suffix(".csv")
                        new_path = diretorio / new_name
                        extracted_path.rename(new_path)

                        sequencial_name = f"socios{contador_csv:02d}.csv"
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

def aplicar_schema(diretorio: Path, colunas: list[str]):
    csv_files = sorted(diretorio.glob("socios*.csv"))
    final_parquet_path = diretorio / "socios_final.parquet"

    if final_parquet_path.exists():
        final_parquet_path.unlink()

    all_chunks = []

    for i, csv_file in enumerate(csv_files):
        try:
            chunk_iter = pd.read_csv(
                csv_file,
                sep=';',
                header=None,
                dtype=str,
                encoding='latin1',
                chunksize=100_000
            )

            for chunk in chunk_iter:
                chunk.columns = colunas
                all_chunks.append(chunk)

            print(f"Processado: {csv_file.name}")

        except Exception as e:
            print(f"Erro ao processar {csv_file.name}: {e}")

    if all_chunks:
        final_df = pd.concat(all_chunks, ignore_index=True)
        final_df.to_parquet(final_parquet_path, index=False)
        print(f"Dados combinados salvos em: {final_parquet_path.name}")
    else:
        print("Nenhum dado foi processado para salvar no arquivo Parquet.")

    for csv_file in csv_files:
        try:
            csv_file.unlink()
            print(f"Removido arquivo intermediário: {csv_file.name}")
        except Exception as e:
            print(f"Erro ao remover {csv_file.name}: {e}")

def getSocios():
    today = datetime.today()
    previousMonth = today - relativedelta(months=1)
    filename = previousMonth.strftime('%Y-%m')

    root_dir = Path(__file__).resolve().parent.parent
    output_dir = root_dir / "Data"
    output_dir.mkdir(parents=True, exist_ok=True)

    print("Tentando baixar bases de Sócios...")
    contador = 0
    while True:
        try:
            base_url = f"https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/{filename}/Socios{contador}.zip"
            zip_path = output_dir / f"socios_{filename}_{contador}.zip"

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
    aplicar_schema(output_dir, COLUMNS)
    print("Processamento concluído.")
    