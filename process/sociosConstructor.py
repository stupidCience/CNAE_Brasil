import pandas as pd
import os
from tqdm import tqdm

SOCIOS_SCHEMA = [
    "id",
    "descricao"
]
PAISES_SCHEMA = ["id_pais", "descricao_pais"]

def sociosConstructor():
    print("Iniciando o processamento de sócios...")

    if os.path.exists("./database/socios.parquet"):
        os.remove("./database/socios.parquet")
        print("Arquivo existente removido.")

    steps = [
        "Carregando tabelas auxiliares",
        "Carregando dados principais",
        "Convertendo datas",
        "Join com qualificações de sócios",
        "Join com qualificações de representante",
        "Join com identificador de sócio",
        "Join com faixa etária",
        "Join com país",
        "Ajustando colunas finais",
        "Removendo colunas auxiliares",
        "Exportando banco de dados"
    ]

    for step in tqdm(steps, desc="Etapas do processamento"):
        if step == "Carregando tabelas auxiliares":
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
                nrows=1000,
                names=PAISES_SCHEMA,
                header=None
            )
        elif step == "Carregando dados principais":
            socios = pd.read_parquet(
                "./Data/socios_final.parquet",
                filters=[('qualificacao_representante', '!=', '00')]
                
            )
        elif step == "Convertendo datas":
            socios['data_entrada_sociedade'] = pd.to_datetime(socios['data_entrada_sociedade'], errors='coerce').dt.date
        elif step == "Join com qualificações de sócios":
            socios = socios.merge(
                qualificacoes[['id', 'descricao']],
                left_on='qualificacao_socio',
                right_on='id',
                how='left'
            ).rename(columns={'descricao': 'descricao_qualificacao_representante', 'id': 'id_qualificacao_representante'})
        elif step == "Join com qualificações de representante":
            socios = socios.merge(
                qualificacoes[['id', 'descricao']],
                left_on='qualificacao_representante',
                right_on='id',
                how='left'
            ).rename(columns={'descricao': 'descricao_qualificacao_socio', 'id': 'id_qualificacao_socio'})
        elif step == "Join com identificador de sócio":
            socios = socios.merge(
                identificador_socio,
                left_on='identificador_socio',
                right_on='codigo',
                how='left'
            )
        elif step == "Join com faixa etária":
            socios = socios.merge(
                faixa_etaria,
                left_on='faixa_etaria',
                right_on='codigo_faixa',
                how='left'
            )
        elif step == "Join com país":
            socios = socios.merge(
                pais,
                left_on='pais',
                right_on='id_pais',
                how='left'
            )
        elif step == "Ajustando colunas finais":
            socios['qualificacao_representante'] = socios['descricao_qualificacao_representante']
            socios['qualificacao_socio'] = socios['descricao_qualificacao_socio']
            socios['identificador_socio'] = socios['descricao_identificador']
            socios['pais'] = socios['descricao_pais']
            socios['faixa_etaria'] = socios['descricao_faixa']
        elif step == "Removendo colunas auxiliares":
            socios = socios.drop(columns=[
                'descricao_qualificacao_socio',
                'descricao_qualificacao_representante',
                'descricao_identificador',
                'id_qualificacao_representante',
                'id_qualificacao_socio',
                'codigo_faixa',
                'codigo',
                'id_pais',
                'descricao_pais',
                'descricao_faixa'
            ], errors='ignore')
        elif step == "Exportando banco de dados":
            export_path = "./database/socios.parquet"
            socios.to_parquet(export_path, index=False)
            print(f"Dados exportados para {export_path}")
   
    print("Processamento de sócios concluído.")
