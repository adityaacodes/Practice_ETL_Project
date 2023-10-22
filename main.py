from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from datetime import datetime
import configparser
import psycopg2
from sqlalchemy import create_engine
import requests
import os


def connect_to_db(password):
    # Connecting to database
    con = psycopg2.connect(database='World_Economies',
                           user='postgres',
                           password=password,
                           host='127.0.0.1',
                           port='5432')

    # Creating Engine
    engine = create_engine(f"postgresql+psycopg2://postgres:{password}@localhost:5432/World_Economies")

    return con, engine


def extract(url, table_attribs):
    response = requests.get(url).text
    soup = BeautifulSoup(response, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = soup.find_all('tbody')
    rows = tables[2].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            if col[0].find('a') is not None and '—' not in col[2]:
                data_dict = {"Country": col[0].a.contents[0],
                             "GDP_USD_millions": col[2].contents[0]}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df, df1], ignore_index=True)
    return df


def transform(df):
    ''' This function converts the GDP information from Currency
        format to float value, transforms the information of GDP from
        USD (Millions) to USD (Billions) rounding to 2 decimal places.
        The function returns the transformed dataframe.'''
    GDP_list = df["GDP_USD_millions"].tolist()
    GDP_list = [float("".join(x.split(','))) for x in GDP_list]
    GDP_list = [np.round(x / 1000, 2) for x in GDP_list]
    df["GDP_USD_millions"] = GDP_list
    df = df.rename(columns={"GDP_USD_millions": "gdp_usd_billions"})
    return df


def load_to_csv(df, csv_path):
    df.to_csv(csv_path)


def load_to_db(engine, transformed_data, table_name):
    transformed_data.to_sql(table_name, engine, if_exists='replace', index=False)


def run_query(statement, engine):
    return pd.read_sql(statement, engine)


def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'  # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now()  # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open(log_file, 'a') as f:
        f.write(timestamp + ',' + message + '\n')


url = "https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29"
table_attribs = ['Country', 'GDP_USD_millions']
csv_path = os.path.join(os.getcwd(), "transformed_data.csv")
table_name = 'countries_by_gdp'
log_file = 'log_file.txt'


# Getting the PostgreSQL password from the config.ini file
config = configparser.ConfigParser()
config.read('config.ini')
password = config['secret']['PASSWORD']


# Log the initialization of the ETL process
log_progress('Preliminaries complete. Initiating ETL process')

# Log the beginning of the Extraction process
df = extract(url, table_attribs)

# Log the completion of the Extraction process and the beginning of the Transformation process
log_progress('Data extraction complete. Initiating Transformation process')
df = transform(df)

# Log the completion of the Transformation process and the beginning of the Loading process
log_progress('Data transformation complete. Initiating loading process')
load_to_csv(df, csv_path)
log_progress('Data saved to CSV file')
con, engine = connect_to_db(password)
log_progress('SQL Connection initiated.')
load_to_db(engine, df, table_name)

# Log the completion of the Loading process and run an Example query
log_progress('Data loaded to Database as table. Running the query')
query_statement = f'SELECT * from {table_name} WHERE gdp_usd_billions >= 100'
print(run_query(query_statement, con))

# Log the completion of the ETL process
log_progress('Process Complete.')

con.close()
