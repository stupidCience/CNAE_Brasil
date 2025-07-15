import pandas as pd


data = pd.read_parquet('.\database\estabelecimentos.parquet', nrows=1000)


pd.set_option('display.max_columns', None)
print(data.head())