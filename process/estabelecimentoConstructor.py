import pandas as pd
import os

MOTIVOS_SCHEMA = [
    "id_motivo",
    "descricao_motivo"
]
PAIS_SCHEMA = [
    "id_pais",
    "descricao_pais"
]
CNAES_SCHEMA = [
    "id_cnae",
    "descricao_cnae"
]

def estabelecimentoConstructor():
    print("Iniciando o processamento de estabelecimentos...")

    if os.path.exists("./database/estabelecimentos.csv"):
        os.remove("./database/estabelecimentos.csv")
        print("Arquivo existente removido.")

    print("Carregando os dados...")
        

    estabelecimentos_path = "./Data/estabelecimentos_final.csv"
    motivos_situacao_cadastral_path = "./Auxiliar/motivos.csv"
    pais_path = "./Auxiliar/paises.csv"
    cnaes_path = "./Auxiliar/cnaes.csv"


    estabelecimentos = pd.read_csv(estabelecimentos_path, sep=';', dtype=str, encoding='latin1', nrows=1000)
    motivos_situacao_cadastral = pd.read_csv(motivos_situacao_cadastral_path, sep=';', dtype=str, encoding='latin1', names=MOTIVOS_SCHEMA, header=None)
    identificador_matriz = pd.DataFrame({
        "codigo_matriz": ["1", "2"],
        "descricao_matriz": ["MATRIZ", "FILIAL"]
    })
    situacao_cadastral = pd.DataFrame({
        "codigo_situacao": ["01", "2", "3", "4", "08"],
        "descricao_situacao_cadastral": ["NULA", "ATIVA", "SUSPENSA", "INAPTA", "BAIXADA"]
    })
    pais = pd.read_csv(pais_path, sep=';', dtype=str, encoding='latin1', names=PAIS_SCHEMA, header=None)
    cnaes = pd.read_csv(cnaes_path, sep=';', dtype=str, encoding='latin1', names=CNAES_SCHEMA, header=None)




    estabelecimentos['data_situacao_cadastral'] = pd.to_datetime(estabelecimentos['data_situacao_cadastral'], errors='coerce').dt.date
    estabelecimentos['data_inicio_atividade'] = pd.to_datetime(estabelecimentos['data_inicio_atividade'], errors='coerce').dt.date
    estabelecimentos['data_situacao_especial'] = pd.to_datetime(estabelecimentos['data_situacao_especial'], errors='coerce').dt.date

    estabelecimentos['CNPJ'] = (
        estabelecimentos['cnpj_basico'].astype(str) +
        estabelecimentos['cnpj_ordem'].astype(str) +
        estabelecimentos['cnpj_dv'].astype(str)
    )

    cols = ['CNPJ'] + [col for col in estabelecimentos.columns if col != 'CNPJ']
    estabelecimentos = estabelecimentos[cols]

    estabelecimentos = estabelecimentos.merge(
        identificador_matriz, 
        left_on='identificador_matriz',
        right_on='codigo_matriz', 
        how='left'
    )
    estabelecimentos = estabelecimentos.merge(
        situacao_cadastral, 
        left_on='situacao_cadastral',
        right_on='codigo_situacao',
        how='left'
    )
    estabelecimentos = estabelecimentos.merge(
        motivos_situacao_cadastral,
        left_on='motivo_situacao_cadastral',
        right_on='id_motivo',
        how='left'
    )
    estabelecimentos = estabelecimentos.merge(
        pais,
        left_on='pais',
        right_on='id_pais',
        how='left'
    )
    estabelecimentos = estabelecimentos.merge(
        cnaes,
        left_on='cnae_fiscal_principal',
        right_on='id_cnae',
        how='left'
    )

    estabelecimentos['motivo_situacao_cadastral'] = estabelecimentos['descricao_motivo']
    estabelecimentos['identificador_matriz'] = estabelecimentos['descricao_matriz']
    estabelecimentos['situacao_cadastral'] = estabelecimentos['descricao_situacao_cadastral']
    estabelecimentos['pais'] = estabelecimentos['descricao_pais']
    estabelecimentos['cnae_fiscal_principal'] = estabelecimentos['descricao_cnae']

    estabelecimentos.drop(
        columns=['codigo_matriz',
                'descricao_matriz',
                'codigo_situacao',
                'descricao_situacao_cadastral',
                'id_motivo',
                'descricao_motivo',
                'id_pais',
                'descricao_pais',
                'id_cnae',
                'descricao_cnae',
                'motivo_situacao_cadastral.1',
                'cnpj_basico',
                'cnpj_ordem',
                'cnpj_dv'
                ],
        inplace=True
    )

    print("Processamento concluído. Salvando os dados...")
    print("Exportando para CSV...")

    export_path = "./database/estabelecimentos.csv"
    estabelecimentos.to_csv(export_path, sep=';', index=False, encoding='utf-8')

    print("Dados exportados com sucesso para", export_path)
    print("Processamento de estabelecimentos concluído.")