import pandas as pd
import sqlite3

# Step 1: Read data from the CSV file
data_url = "https://download-data.deutschebahn.com/static/datasets/haltestellen/D_Bahnhof_2020_alle.CSV"
df = pd.read_csv(data_url, sep=';')

# Step 2: Drop the "Status" column
df.drop("Status", axis=1, inplace=True)

# Step 3: Filter rows with valid values
Verkehr_list = ["FV", "RV", "nur DPN"]
df = df[df['Verkehr'].isin(Verkehr_list)]

df['Laenge'] = pd.to_numeric(df['Laenge'].str.replace(',', '.'), errors='coerce')
df['Breite'] = pd.to_numeric(df['Breite'].str.replace(',', '.'), errors='coerce')

df = df[(df['Laenge'] >= -90) & (df['Laenge'] <= 90)]
df = df[(df['Breite'] >= -90) & (df['Breite'] <= 90)]

pattern = r"^[A-Za-z]{2}:\d*:\d*(?::\d*)?$"
df = df[df['IFOPT'].str.match(pattern, na=False)]

# Step 4: Create SQLite database and insert data
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

# Create the "trainstops" table
create_table_query = f"CREATE TABLE trainstops ({', '.join([f'{col} {sqlite_types[col]}' for col in df.columns])})"
cursor.execute(create_table_query)

# Insert data into the "trainstops" table
df.to_sql('trainstops', conn, if_exists='replace', index=False)

# Commit changes and close the connection
conn.commit()
conn.close()
