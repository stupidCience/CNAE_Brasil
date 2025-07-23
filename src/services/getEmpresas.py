import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pathlib import Path
import sys
import zipfile
import pandas as pd

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
    
    url_base = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/{ano}-{mes:02d}/"
    diretorio_download = Path("Data")
    diretorio_download.mkdir(exist_ok=True)
    
    data_atual = datetime.now()
    
    for i in range(11):
        data_download = data_atual - relativedelta(months=i)
        ano = data_download.year
        mes = data_download.month
        
        print(f"📅 Tentando {ano}-{mes:02d}...")
        
        for j in range(0, 12):
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
    """Processa arquivos CSV de empresas e consolida em um único arquivo usando chunks"""
    print("⚙️ Processando arquivos de empresas com chunks...")
    
    diretorio = Path("Data")
    arquivos_csv = list(diretorio.glob("empresas*.csv"))
    
    if not arquivos_csv:
        print("❌ Nenhum arquivo CSV de empresas encontrado")
        return
    
    caminho_saida = Path("database") / "empresas_final.csv"
    caminho_saida.parent.mkdir(exist_ok=True)
    
    # Remove arquivo existente se houver
    if caminho_saida.exists():
        caminho_saida.unlink()
    
    total_registros = 0
    chunk_size = 50000  # Processa 50k registros por vez
    
    print(f"📁 Encontrados {len(arquivos_csv)} arquivos para processar")
    
    # Processa cada arquivo em chunks
    for i, arquivo in enumerate(arquivos_csv, 1):
        print(f"📄 Processando arquivo {i}/{len(arquivos_csv)}: {arquivo.name}")
        
        try:
            # Lê o arquivo em chunks
            chunk_iter = pd.read_csv(
                arquivo,
                sep=';',
                header=None,
                names=COLUMNS,
                dtype=str,
                encoding='latin1',
                on_bad_lines='skip',
                chunksize=chunk_size
            )
            
            arquivo_registros = 0
            
            for chunk_num, chunk in enumerate(chunk_iter, 1):
                # Salva o chunk no arquivo final
                chunk.to_csv(
                    caminho_saida, 
                    mode='a',  # Modo append
                    header=(total_registros == 0),  # Header apenas no primeiro chunk
                    index=False, 
                    sep=';', 
                    encoding='utf-8'
                )
                
                arquivo_registros += len(chunk)
                total_registros += len(chunk)
                
                # Mostra progresso
                print(f"  📊 Chunk {chunk_num}: +{len(chunk):,} registros (Total: {total_registros:,})")
            
            print(f"  ✅ {arquivo.name}: {arquivo_registros:,} registros processados")
            
        except Exception as e:
            print(f"❌ Erro ao processar {arquivo}: {e}")
            continue
    
    if total_registros > 0:
        print(f"✅ Consolidação concluída!")
        print(f"📊 Total de registros processados: {total_registros:,}")
        print(f"💾 Arquivo salvo: {caminho_saida}")
        
        # Remove arquivos CSV após processamento
        print("🧹 Limpando arquivos temporários...")
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
