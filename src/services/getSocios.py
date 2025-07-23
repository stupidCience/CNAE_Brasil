import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pathlib import Path
import sys
import zipfile
import pandas as pd
from tqdm import tqdm

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
from src.schemas.sociosSchema import SOCIOS_SCHEMA as COLUMNS

def extrair_e_limpar_socios(diretorio: Path):
    zips = list(diretorio.glob("*Socios*.zip"))
    contador_csv = 1

    for zip_path in zips:
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                for file_info in zip_ref.infolist():
                    extracted_path = diretorio / file_info.filename
                    
                    if file_info.filename.upper().endswith('.SOCIOCSV'):
                        zip_ref.extract(file_info, diretorio)
                        
                        novo_nome = diretorio / f"socios{contador_csv}.csv"
                        extracted_path.rename(novo_nome)
                        contador_csv += 1
                        print(f"🔄 Arquivo extraído e renomeado: {novo_nome}")
                        
        except Exception as e:
            print(f"❌ Erro ao extrair {zip_path}: {e}")
        finally:
            zip_path.unlink()

def baixar_arquivos_socios():
    print("👥 Baixando arquivos de sócios...")
    
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
            url = f"{url_base}Socios{j}.zip".format(ano=ano, mes=mes)
            nome_arquivo = f"Socios{j}_{ano}_{mes:02d}.zip"
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
        
        if any((diretorio_download / f"Socios{j}_{ano}_{mes:02d}.zip").exists() for j in range(1, 11)):
            print(f"✅ Encontrados arquivos para {ano}-{mes:02d}")
            break
    else:
        print("❌ Nenhum arquivo de sócios encontrado nos últimos 11 meses")

def processar_socios():
    """Processa arquivos CSV de sócios e consolida em um único arquivo"""
    print("⚙️ Processando arquivos de sócios...")
    
    diretorio = Path("Data")
    arquivos_csv = list(diretorio.glob("socios*.csv"))
    
    if not arquivos_csv:
        print("❌ Nenhum arquivo CSV de sócios encontrado")
        return
    
    dataframes = []
    
    for arquivo in tqdm(arquivos_csv, desc="Processando sócios"):
        try:
            df = pd.read_csv(
                arquivo,
                sep=';',
                header=None,
                names=COLUMNS,
                dtype=str,
                encoding='latin1',
                on_bad_lines='skip'
            )
            dataframes.append(df)
            
        except Exception as e:
            print(f"❌ Erro ao processar {arquivo}: {e}")
    
    if dataframes:
        df_consolidado = pd.concat(dataframes, ignore_index=True)
        
        caminho_saida = Path("database") / "socios_final.csv"
        caminho_saida.parent.mkdir(exist_ok=True)
        
        df_consolidado.to_csv(caminho_saida, index=False, sep=';', encoding='utf-8')
        print(f"✅ Arquivo consolidado salvo: {caminho_saida}")
        print(f"📊 Total de registros: {len(df_consolidado):,}")
        
        for arquivo in arquivos_csv:
            arquivo.unlink()
            
    else:
        print("❌ Nenhum arquivo foi processado com sucesso")

def baixar_socios():
    """Função principal para baixar e processar dados de sócios"""
    baixar_arquivos_socios()
    extrair_e_limpar_socios(Path("Data"))
    processar_socios()

# Mantém compatibilidade com código antigo
getSocios = baixar_socios
