import pandas as pd
import os

NATUREZA_SCHEMA = ["id_natureza", "descricao_natureza"]

def empresasConstructor():
    print("Iniciando o processamento de empresas...")

    if os.path.exists("./database/empresas.csv"):
        os.remove("./database/empresas.csv")
        print("Arquivo existente removido.")

    print("Carregando os dados...")    


    empresa_path = "./Data/empresas_final.csv"
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
        nrows=1000,
        names=NATUREZA_SCHEMA,
        header=None
    )

    empresas = pd.read_csv(empresa_path, sep=';', dtype=str, encoding='latin1', nrows=1000)

    # Join com Natureza Jurídica
    empresas = empresas.merge(
        natureza,
        left_on='natureza_juridica',
        right_on='id_natureza',
        how='left'
    )

    #Join com Porte
    empresas = empresas.merge(
        porte,
        left_on='porte_empresa',
        right_on='id_porte',
        how='left'
    )



    # Corrigido: use a série, não DataFrame
    empresas['natureza_juridica'] = empresas['descricao_natureza']
    empresas['porte_empresa'] = empresas['descricao_porte']

    empresas.drop(
        columns=['id_natureza', 'descricao_natureza', 
                'id_porte', 'descricao_porte'],
        inplace=True
    )

    print("Processamento concluído. Exportando para CSV...")

    export_path = "./database/empresas.csv"
    empresas.to_csv(export_path, sep=';', index=False, encoding='utf-8')
    print(f"Dados exportados para {export_path}")
    print("Processamento de empresas concluído.")
