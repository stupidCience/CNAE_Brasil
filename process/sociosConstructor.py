import pandas as pd
import os


SOCIOS_SCHEMA = [
    "id",
    "descricao"
]
PAISES_SCHEMA = ["id_pais", "descricao_pais"]

def sociosConstructor():
    print("Iniciando o processamento de sócios...")

    if os.path.exists("./database/socios.csv"):
        os.remove("./database/socios.csv")
        print("Arquivo existente removido.")

    print("Carregando os dados...")

    socios_path = "./Data/socios_final.csv"
    qualificacoes_path = "./Auxiliar/qualificacoes.csv"
    pais_path = "./Auxiliar/paises.csv"

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
            qualificacoes_path,
            sep=';',
            dtype=str,
            encoding='latin1',
            nrows=1000,
            names=SOCIOS_SCHEMA,
            header=None
        )
    pais = pd.read_csv(
            pais_path,
            sep=';',
            dtype=str,
            encoding='latin1',
            nrows=1000,
            names=PAISES_SCHEMA,
            header=None
        )

    socios = pd.read_csv(socios_path, sep=';', dtype=str, encoding='latin1', nrows=1000)


    socios['data_entrada_sociedade'] = pd.to_datetime(socios['data_entrada_sociedade'], errors='coerce').dt.date

    #Join com qualificações de socios
    socios = socios.merge(
        qualificacoes[['id', 'descricao']],
        left_on='qualificacao_socio',
        right_on='id',
        how='left'
    ).rename(columns={'descricao': 'descricao_qualificacao_representante', 'id': 'id_qualificacao_representante'})

    #join com qualificao de representante
    socios = socios.merge(
        qualificacoes[['id', 'descricao']],
        left_on='qualificacao_representante',
        right_on='id',
        how='left'
    ).rename(columns={'descricao': 'descricao_qualificacao_socio', 'id': 'id_qualificacao_socio'})

    #join com identificador de socio
    socios = socios.merge(
        identificador_socio,
        left_on='identificador_socio',
        right_on='codigo',
        how='left'
    )

    #join com faixa etária
    socios = socios.merge(
        faixa_etaria,
        left_on='faixa_etaria',
        right_on='codigo_faixa',
        how='left'
    )

    #join com pais
    socios = socios.merge(
        pais,
        left_on='pais',
        right_on='id_pais',
        how='left'
    )

    socios['qualificacao_representante'] = socios['descricao_qualificacao_representante']
    socios['qualificacao_socio'] = socios['descricao_qualificacao_socio']
    socios['identificador_socio'] = socios['descricao_identificador']
    socios['pais'] = socios['descricao_pais']
    socios['faixa_etaria'] = socios['descricao_faixa']

    # Remove colunas auxiliares
    socios = socios.drop(columns=
                            [
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

        #pd.set_option('display.max_columns', None)
        #print(socios.head(5))

    print("Processamento de sócios concluído.")
    print("Exportando para CSV...")

    export_path = "./database/socios.csv"
    socios.to_csv(export_path, sep=';', index=False, encoding='utf-8')
    print(f"Dados exportados para {export_path}")
    print("Processamento de sócios concluído.")

