import pandas as pd

table_path = "./Data/empresas_final.csv"

for chunk in pd.read_csv(table_path, sep=";", chunksize=10000):
    table = pd.DataFrame(chunk)
    print(table.head())
    break
