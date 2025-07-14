import pandas as pd
import os
from tqdm import tqdm

NATUREZA_SCHEMA = ["id_natureza", "descricao_natureza"]

def empresasConstructor():
    print("Iniciando o processamento de empresas...")

    if os.path.exists("./database/empresas.csv"): # Atenção: aqui ainda está .csv
        os.remove("./database/empresas.csv")
        print("Arquivo existente removido.")

    steps = [
        "Carregando os dados",
        "Filtrando CNPJ básico", # Adicionei esta etapa para clareza
        "Join com Natureza Jurídica",
        "Join com Porte",
        "Ajustando colunas finais",
        "Removendo colunas auxiliares",
        "Exportando banco de dados"
    ]

    # Inicializa 'empresas' antes do loop para garantir que esteja acessível
    empresas = pd.DataFrame() 

    for step in tqdm(steps, desc="Etapas do processamento"):
        if step == "Carregando os dados":
            # Assumindo que empresas_final.parquet é o arquivo gerado anteriormente
            empresa_path = "./Data/empresas_final.parquet" 
            natureza_path = "./Auxiliar/naturezas.csv"

            porte = pd.DataFrame({
                "id_porte": ["00","01", "03", "05"],
                "descricao_porte": ["NÃO INFORMADO", "MICROEMPRESA", "EMPRESA DE PEQUENO PORTE", "DEMAIS"]
            })

            natureza = pd.read_csv(
                natureza_path,
                sep=';',
                dtype=str,
                encoding='latin1',
                names=NATUREZA_SCHEMA,
                header=None
            )

            # Carrega o arquivo Parquet completo.
            # O Parquet já é um formato otimizado para leitura.
            empresas = pd.read_parquet(empresa_path)
            
        elif step == "Filtrando CNPJ básico":
            # Aplica os filtros APÓS o carregamento do DataFrame
            # Filtro para 'cnpj_basico':
            # Diferente de '00000000' E diferente de '00000000-0000'
            # E não é nulo (usando .notna())
            
            # Garante que a coluna 'cnpj_basico' é string para o filtro
            if 'cnpj_basico' in empresas.columns:
                empresas['cnpj_basico'] = empresas['cnpj_basico'].astype(str)
                condicao_cnpj_valido = (
                    (empresas['cnpj_basico'] != '00000000') &
                    (empresas['cnpj_basico'] != '00000000-0000') &
                    (empresas['cnpj_basico'].notna())
                )
                empresas = empresas[condicao_cnpj_valido]
            else:
                print("Atenção: 'cnpj_basico' não encontrada para filtragem.")

        elif step == "Join com Natureza Jurídica":
            empresas = empresas.merge(
                natureza,
                left_on='natureza_juridica',
                right_on='id_natureza',
                how='left'
            )
        elif step == "Join com Porte":
            empresas = empresas.merge(
                porte,
                left_on='porte_empresa',
                right_on='id_porte',
                how='left'
            )
        elif step == "Ajustando colunas finais":
            # Garante que 'descricao_natureza' e 'descricao_porte' existem após os joins
            if 'descricao_natureza' in empresas.columns:
                empresas['natureza_juridica'] = empresas['descricao_natureza']
            if 'descricao_porte' in empresas.columns:
                empresas['porte_empresa'] = empresas['descricao_porte']
                
        elif step == "Removendo colunas auxiliares":
            empresas.drop(
                columns=['id_natureza', 'descricao_natureza', 
                         'id_porte', 'descricao_porte'],
                inplace=True,
                errors='ignore'
            )
        elif step == "Exportando banco de dados":
            # Ajustando para .parquet no destino final
            export_path = "./database/empresas.parquet"
            empresas.to_parquet(export_path, index=False)
            print(f"Dados exportados para {export_path}")

    print("Processamento de empresas concluído.")
    