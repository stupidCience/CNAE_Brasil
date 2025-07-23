import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pathlib import Path
import sys
import zipfile
import pandas as pd
from tqdm import tqdm

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
from src.schemas.empSchema import EMPRESAS_SCHEMA as COLUMNS

def extrair_e_limpar(diretorio: Path):
    zips = list(diretorio.glob("*.zip"))
    contador_csv = 1

    for zip_path in zips:
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                for file_info in zip_ref.infolist():
                    extracted_path = diretorio / file_info.filename
                    
                    if file_info.filename.endswith('.EMPRECSV'):
                        zip_ref.extract(file_info, diretorio)
                        
                        novo_nome = diretorio / f"empresas{contador_csv}.csv"
                        extracted_path.rename(novo_nome)
                        contador_csv += 1
                        print(f"🔄 Arquivo extraído e renomeado: {novo_nome}")
                        
        except Exception as e:
            print(f"❌ Erro ao extrair {zip_path}: {e}")
        finally:
            zip_path.unlink()

def baixar_arquivos_empresas():
    print("🏢 Baixando arquivos de empresas...")
    
    url_base = "http://200.152.38.155/CNPJ/dados_abertos_cnpj/{ano}-{mes:02d}/"
    diretorio_download = Path("Data")
    diretorio_download.mkdir(exist_ok=True)
    
    data_atual = datetime.now()
    
    for i in range(11):
        data_download = data_atual - relativedelta(months=i)
        ano = data_download.year
        mes = data_download.month
        
        print(f"📅 Tentando {ano}-{mes:02d}...")
        
        for j in range(1, 12):
            url = f"{url_base}Empresas{j}.zip".format(ano=ano, mes=mes)
            nome_arquivo = f"Empresas{j}_{ano}_{mes:02d}.zip"
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
        
        if any((diretorio_download / f"Empresas{j}_{ano}_{mes:02d}.zip").exists() for j in range(1, 12)):
            print(f"✅ Encontrados arquivos para {ano}-{mes:02d}")
            break
    else:
        print("❌ Nenhum arquivo de empresas encontrado nos últimos 11 meses")

def processar_empresas():
    """Processa arquivos CSV de empresas e consolida em um único arquivo"""
    print("⚙️ Processando arquivos de empresas...")
    
    diretorio = Path("Data")
    arquivos_csv = list(diretorio.glob("empresas*.csv"))
    
    if not arquivos_csv:
        print("❌ Nenhum arquivo CSV de empresas encontrado")
        return
    
    dataframes = []
    
    for arquivo in tqdm(arquivos_csv, desc="Processando empresas"):
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
        
        caminho_saida = Path("database") / "empresas_final.csv"
        caminho_saida.parent.mkdir(exist_ok=True)
        
        df_consolidado.to_csv(caminho_saida, index=False, sep=';', encoding='utf-8')
        print(f"✅ Arquivo consolidado salvo: {caminho_saida}")
        print(f"📊 Total de registros: {len(df_consolidado):,}")
        
        for arquivo in arquivos_csv:
            arquivo.unlink()
            
    else:
        print("❌ Nenhum arquivo foi processado com sucesso")

def baixar_empresas():
    """Função principal para baixar e processar dados de empresas"""
    baixar_arquivos_empresas()
    extrair_e_limpar(Path("Data"))
    processar_empresas()

# Mantém compatibilidade com código antigo
getEmp = baixar_empresas
