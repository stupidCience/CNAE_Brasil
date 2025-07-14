import pandas as pd
import os
from tqdm import tqdm

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

    # Ajuste: Remova o arquivo .parquet se ele existir
    if os.path.exists("./database/estabelecimentos.parquet"):
        os.remove("./database/estabelecimentos.parquet")
        print("Arquivo existente removido.")

    steps = [
        "Carregando os dados",
        "Filtrando CNPJ básico e situação cadastral", # Nova etapa para filtros
        "Convertendo datas",
        "Criando coluna CNPJ",
        "Join com identificador matriz",
        "Join com situação cadastral",
        "Join com motivo situação cadastral",
        "Join com país",
        "Join com CNAE",
        "Ajustando colunas finais",
        "Removendo colunas auxiliares",
        "Exportando banco de dados"
    ]

    # Inicializa 'estabelecimentos' para garantir que esteja acessível
    estabelecimentos = pd.DataFrame() 

    for step in tqdm(steps, desc="Etapas do processamento"):
        if step == "Carregando os dados":
            # Caminhos dos arquivos
            estabelecimentos_path = "./Data/estabelecimentos_final.parquet" # Lendo o Parquet final
            motivos_situacao_cadastral_path = "./Auxiliar/motivos.csv"
            pais_path = "./Auxiliar/paises.csv"
            cnaes_path = "./Auxiliar/cnaes.csv"

            # Carrega o DataFrame de estabelecimentos (Parquet)
            estabelecimentos = pd.read_parquet(estabelecimentos_path)
            
            # Carrega DataFrames auxiliares
            motivos_situacao_cadastral = pd.read_csv(motivos_situacao_cadastral_path, sep=';', dtype=str, encoding='utf-8', names=MOTIVOS_SCHEMA, header=None)
            
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

        elif step == "Filtrando CNPJ básico e situação cadastral":
            if 'cnpj_basico' in estabelecimentos.columns:
                estabelecimentos['cnpj_basico'] = estabelecimentos['cnpj_basico'].astype(str).fillna('')
                condicao_cnpj_valido = (
                    (estabelecimentos['cnpj_basico'] != '00000000') &
                    (estabelecimentos['cnpj_basico'] != '00000000-0000') &
                    (estabelecimentos['cnpj_basico'] != '')
                )
            else:
                condicao_cnpj_valido = True # Se a coluna não existe, não filtra por ela
                print("Atenção: 'cnpj_basico' não encontrada para filtragem.")

            # Filtro para 'situacao_cadastral': diferente de '01' OU '04'
            if 'situacao_cadastral' in estabelecimentos.columns:
                estabelecimentos['situacao_cadastral'] = estabelecimentos['situacao_cadastral'].astype(str).fillna('')
                condicao_situacao = ~estabelecimentos['situacao_cadastral'].isin(['01', '04'])
            else:
                condicao_situacao = True # Se a coluna não existe, não filtra por ela
                print("Atenção: 'situacao_cadastral' não encontrada para filtragem.")

            # Combina e aplica os filtros
            if condicao_cnpj_valido is not True and condicao_situacao is not True:
                estabelecimentos = estabelecimentos[condicao_cnpj_valido & condicao_situacao]
            elif condicao_cnpj_valido is not True:
                estabelecimentos = estabelecimentos[condicao_cnpj_valido]
            elif condicao_situacao is not True:
                estabelecimentos = estabelecimentos[condicao_situacao]

        elif step == "Convertendo datas":
            for col in ['data_situacao_cadastral', 'data_inicio_atividade', 'data_situacao_especial']:
                if col in estabelecimentos.columns:
                    estabelecimentos[col] = pd.to_datetime(estabelecimentos[col], errors='coerce').dt.date
                else:
                    print(f"Atenção: Coluna '{col}' não encontrada para conversão de data.")

        elif step == "Criando coluna CNPJ":
            # Garante que as colunas existem antes de concatenar
            if all(col in estabelecimentos.columns for col in ['cnpj_basico', 'cnpj_ordem', 'cnpj_dv']):
                estabelecimentos['CNPJ'] = (
                    estabelecimentos['cnpj_basico'].astype(str) +
                    estabelecimentos['cnpj_ordem'].astype(str) +
                    estabelecimentos['cnpj_dv'].astype(str)
                )
                # Reordena colunas para ter CNPJ no início
                cols = ['CNPJ'] + [col for col in estabelecimentos.columns if col != 'CNPJ']
                estabelecimentos = estabelecimentos[cols]
            else:
                print("Atenção: Colunas necessárias para 'CNPJ' não encontradas.")

        elif step == "Join com identificador matriz":
            if 'identificador_matriz' in estabelecimentos.columns:
                estabelecimentos = estabelecimentos.merge(
                    identificador_matriz, 
                    left_on='identificador_matriz',
                    right_on='codigo_matriz', 
                    how='left'
                )
            else:
                print("Atenção: Coluna 'identificador_matriz' não encontrada para join.")

        elif step == "Join com situação cadastral":
            if 'situacao_cadastral' in estabelecimentos.columns:
                estabelecimentos = estabelecimentos.merge(
                    situacao_cadastral, 
                    left_on='situacao_cadastral',
                    right_on='codigo_situacao',
                    how='left'
                )
            else:
                print("Atenção: Coluna 'situacao_cadastral' não encontrada para join.")
                
        elif step == "Join com motivo situação cadastral":
            if 'motivo_situacao_cadastral' in estabelecimentos.columns:
                estabelecimentos = estabelecimentos.merge(
                    motivos_situacao_cadastral,
                    left_on='motivo_situacao_cadastral',
                    right_on='id_motivo',
                    how='left'
                )
            else:
                print("Atenção: Coluna 'motivo_situacao_cadastral' não encontrada para join.")

        elif step == "Join com país":
            if 'pais' in estabelecimentos.columns:
                estabelecimentos = estabelecimentos.merge(
                    pais,
                    left_on='pais',
                    right_on='id_pais',
                    how='left'
                )
            else:
                print("Atenção: Coluna 'pais' não encontrada para join.")

        elif step == "Join com CNAE":
            if 'cnae_fiscal_principal' in estabelecimentos.columns:
                estabelecimentos = estabelecimentos.merge(
                    cnaes,
                    left_on='cnae_fiscal_principal',
                    right_on='id_cnae',
                    how='left'
                )
            else:
                print("Atenção: Coluna 'cnae_fiscal_principal' não encontrada para join.")

        elif step == "Ajustando colunas finais":
            # Ajusta colunas finais apenas se as colunas de descrição existirem após os joins
            if 'descricao_motivo' in estabelecimentos.columns:
                estabelecimentos['motivo_situacao_cadastral'] = estabelecimentos['descricao_motivo']
            if 'descricao_matriz' in estabelecimentos.columns:
                estabelecimentos['identificador_matriz'] = estabelecimentos['descricao_matriz']
            if 'descricao_situacao_cadastral' in estabelecimentos.columns:
                estabelecimentos['situacao_cadastral'] = estabelecimentos['descricao_situacao_cadastral']
            if 'descricao_pais' in estabelecimentos.columns:
                estabelecimentos['pais'] = estabelecimentos['descricao_pais']
            if 'descricao_cnae' in estabelecimentos.columns:
                estabelecimentos['cnae_fiscal_principal'] = estabelecimentos['descricao_cnae']
                
        elif step == "Removendo colunas auxiliares":
            # Lista de colunas a remover, com `errors='ignore'` para não quebrar se uma coluna não existir
            cols_to_drop = [
                'codigo_matriz',
                'descricao_matriz',
                'codigo_situacao',
                'descricao_situacao_cadastral',
                'id_motivo',
                'descricao_motivo',
                'id_pais',
                'descricao_pais',
                'id_cnae',
                'descricao_cnae',
                'motivo_situacao_cadastral.1', # Manter caso venha de um merge anterior com sufixo
                'cnpj_basico',
                'cnpj_ordem',
                'cnpj_dv'
            ]
            estabelecimentos.drop(
                columns=[col for col in cols_to_drop if col in estabelecimentos.columns],
                inplace=True,
                errors='ignore' # 'ignore' já é o padrão para drop, mas explicito
            )
            
        elif step == "Exportando banco de dados":
            export_path = "./database/estabelecimentos.parquet"
            estabelecimentos.to_parquet(export_path, index=False)
            print(f"Dados exportados para {export_path}")

    print("Processamento de estabelecimentos concluído.")
