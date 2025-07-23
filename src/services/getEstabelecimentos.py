import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pathlib import Path
import sys
import zipfile
import pandas as pd
from tqdm import tqdm

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
from src.schemas.estabSchema import ESTABELECIMENTOS_SCHEMA as COLUMNS

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
    """Aplica schema aos arquivos CSV de estabelecimentos"""
    all_csv_files = list(diretorio.glob("estabelecimentos*.csv"))
    
    if not all_csv_files:
        print("❌ Nenhum arquivo CSV de estabelecimentos encontrado")
        return None
    
    print(f"📂 Encontrados {len(all_csv_files)} arquivos CSV")
    
    final_df = pd.DataFrame()
    
    for csv_file in tqdm(all_csv_files, desc="Processando estabelecimentos"):
        try:
            chunk_list = []
            
            with pd.read_csv(csv_file, sep=';', header=None, names=colunas, dtype=str, 
                           encoding='latin1', chunksize=chunk_size_csv, on_bad_lines='skip') as reader:
                
                for chunk in reader:
                    chunk_list.append(chunk)
            
            if chunk_list:
                file_df = pd.concat(chunk_list, ignore_index=True)
                final_df = pd.concat([final_df, file_df], ignore_index=True)
                print(f"✅ Processado: {csv_file.name} - {len(file_df):,} registros")
            
        except Exception as e:
            print(f"❌ Erro ao processar {csv_file.name}: {e}")
    
    return final_df

def baixar_arquivos_estabelecimentos():
    print("🏢 Baixando arquivos de estabelecimentos...")
    
    url_base = "http://200.152.38.155/CNPJ/dados_abertos_cnpj/{ano}-{mes:02d}/"
    diretorio_download = Path("Data")
    diretorio_download.mkdir(exist_ok=True)
    
    data_atual = datetime.now()
    
    for i in range(11):
        data_download = data_atual - relativedelta(months=i)
        ano = data_download.year
        mes = data_download.month
        
        print(f"📅 Tentando {ano}-{mes:02d}...")
        
        for j in range(1, 11):
            url = f"{url_base}Estabelecimentos{j}.zip".format(ano=ano, mes=mes)
            nome_arquivo = f"Estabelecimentos{j}_{ano}_{mes:02d}.zip"
            caminho_arquivo = diretorio_download / nome_arquivo
            
            if caminho_arquivo.exists():
                print(f"⚠️ Arquivo {nome_arquivo} já existe. Pulando...")
                continue
            
            try:
                response = requests.get(url, timeout=30)
                if response.status_code == 200:
                    with open(caminho_arquivo, 'wb') as f:
                        f.write(response.content)
                    print(f"✅ {nome_arquivo} baixado com sucesso")
                else:
                    print(f"❌ Erro ao baixar {nome_arquivo}: Status {response.status_code}")
                    
            except requests.RequestException as e:
                print(f"❌ Erro de conexão para {nome_arquivo}: {e}")
                continue
        
        if any((diretorio_download / f"Estabelecimentos{j}_{ano}_{mes:02d}.zip").exists() for j in range(1, 11)):
            print(f"✅ Encontrados arquivos para {ano}-{mes:02d}")
            break
    else:
        print("❌ Nenhum arquivo de estabelecimentos encontrado nos últimos 11 meses")

def baixar_estabelecimentos():
    """Função principal para baixar e processar dados de estabelecimentos"""
    baixar_arquivos_estabelecimentos()
    extrair_e_limpar(Path("Data"))
    
    df_final = aplicar_schema_estabelecimentos(Path("Data"), COLUMNS)
    
    if df_final is not None and not df_final.empty:
        caminho_saida = Path("database") / "estabelecimentos_final.csv"
        caminho_saida.parent.mkdir(exist_ok=True)
        
        df_final.to_csv(caminho_saida, index=False, sep=';', encoding='utf-8')
        print(f"✅ Arquivo final salvo: {caminho_saida}")
        print(f"📊 Total de registros: {len(df_final):,}")
        
        arquivos_csv = list(Path("Data").glob("estabelecimentos*.csv"))
        for arquivo in arquivos_csv:
            arquivo.unlink()
    else:
        print("❌ Nenhum dado foi processado")

# Mantém compatibilidade com código antigo
getEstab = baixar_estabelecimentos
