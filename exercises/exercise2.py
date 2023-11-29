import pandas as pd
import sqlite3

data_url="https://download-data.deutschebahn.com/static/datasets/haltestellen/D_Bahnhof_2020_alle.CSV"
df= pd.read_csv(data_url,sep=';')

df.drop("Status",axis=1,inplace= True)

Verkehr_list=("FV", "RV", "nur DPN")
df = df[df['Verkehr'].isin(Verkehr_list)]

df.Laenge.replace(to_replace=",", value=".", inplace=True,regex=True)
df.Breite.replace(to_replace=",", value=".", inplace=True,regex=True)


df = df.astype({'Laenge':'float'})
df = df.astype({'Breite':'float'})

df = df[(df.Laenge <= 90.0) & (df.Laenge >= -90.0)]



pattern = "^[A-Za-z]{2}:\d*:\d*(?::\d*)?$"
df.IFOPT.str.match(pattern)

df=df[df.IFOPT.str.match(pattern)]

conn = sqlite3.connect('trainstops.sqlite')
cursor = conn.cursor()

# Define SQLite types for each column
sqlite_types = {
    'BFNr': 'INTEGER',
    'DS100': 'TEXT',
    'Name': 'TEXT',
    'Typ': 'TEXT',
    'Verkehr': 'TEXT',
    'Land': 'TEXT',
    'Laenge': 'FLOAT',
    'Breite': 'FLOAT',
    'IFOPT': 'TEXT',
}

create_table_query = f"CREATE TABLE trainstops ({', '.join([f'{col} {sqlite_types[col]}' for col in df.columns])})"
cursor.execute(create_table_query)

df.to_sql('trainstops', conn, if_exists='replace', index=False)

conn.commit()
conn.close()
