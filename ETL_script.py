import pandas as pd
from sqlalchemy import create_engine
import duckdb

def load_data(file):
    return pd.read_parquet(file, engine='pyarrow')

# Function to split complex fields into simple fields, 
# such as phoneNumbers to cell_number and home_number
# and location to lat and lon
def simplify_data(df):
    for i, row in df.iterrows():
        phone_cell=None
        phone_home=None
        phone_gen=None
        try:
            phones = row['phoneNumbers']['phone']
            for phone_info in phones: 
                if phone_info['kind'] == 'cell':
                    phone_cell = phone_info['number']
                elif phone_info['kind'] == 'home': 
                    phone_home = phone_info['number']
                else: 
                    phone_gen = phone_info['number']
            if phone_gen:
                if not phone_home:
                    phone_home = phone_gen
                if not phone_cell and phone_home != phone_gen:
                    phone_cell = phone_gen         
        except:
            pass
        df.at[i, 'cell_number'] = phone_cell
        df.at[i, 'home_number'] = phone_home


        lon=None
        lat = None
        try:
            lon, lat = row['location']['lon'], row['location']['lat']
            if not lon or not lat:
                lon, lat = None, None
        except:
            pass
        df.at[i, 'lon'], df.at[i, 'lat'] = lon, lat
    df.drop(columns=['location', 'phoneNumbers'], inplace=True)


#Function to clean the data: delete if there is no name, no phone at all, duplicates
def clean_data(df):
    df = df.dropna(subset=['name'])
    df = df.dropna(subset=['cell_number', 'home_number'], how='all')
    df = df.drop_duplicates()
    df.reset_index(drop=True, inplace=True)
    return df


# Function to check the data: currently using simple checks, such as name/phone. 
# Entries that don't pass are put in separate df
# Should be implemented with phone format and lat/lon checks
def quality_checks(df):
    phonebook_corrupted = pd.DataFrame(columns=df.columns)
    for i, row in df.iterrows():
        if pd.isna(row['name']) or (pd.isna(row['cell_number']) and pd.isna(row['home_number'])):
            phonebook_corrupted = phonebook_corrupted.append(row, ignore_index=True)
            df.drop(index=i, inplace=True)
    return (df,phonebook_corrupted)


# Function to write to sqlite 
def write_to_sqlite(df, db_name, table_name):
    engine = create_engine(f'sqlite:///databases/{db_name}.db')
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)

# Function to write to duckdb 
def write_to_duckdb(df, db_name, table_name):
    con = duckdb.connect(database=f'databases/{db_name}.duckdb', read_only=False)
    con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")
    con.close()

if __name__ == "__main__":
    df = load_data('datasources/phonebook.parquet')
    simplify_data(df)
    df = clean_data(df)
    df, phonebook_corrupted = quality_checks(df)
    write_to_sqlite(df, 'bragg', 'phonebook')
    write_to_sqlite(phonebook_corrupted, 'bragg', 'phonebook_corrupted')
    write_to_duckdb(df, 'bragg', 'phonebook')
    write_to_duckdb(phonebook_corrupted, 'bragg', 'phonebook_corrupted')

 

