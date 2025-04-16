import requests
import mysql.connector
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import yaml
import sqlalchemy
from sqlalchemy import inspect
import numpy as np

# Load configuration from config.yaml
with open("../config.yaml", "r") as stream:
    config = yaml.safe_load(stream)

# Get paths dynamically from config.yaml
url = config["etl_gdp"]["source"]["url"]
db_name = config["etl_gdp"]["source"]["db_name"]
table_name = config["etl_gdp"]["source"]["table_name"]
table_attribs = config["etl_gdp"]["source"]["table_attribs"]
csv_path = config["etl_gdp"]["source"]["location"]
log_file = config["etl_gdp"]["logging"]["location"]
# # Initialize the known entities
# df = pd.DataFrame(columns=["Country", "GDP (US$)", "Population", "Area (km²)"])


def extract(url, table_attribs):
    ''' This function extracts the required
    information from the website and saves it to a dataframe. The
    function returns the dataframe for further processing. '''

    page = requests.get(url) # Fetch the webpage
    data = BeautifulSoup(page.text, "html.parser") # Parse the webpage
    df = pd.DataFrame(columns=table_attribs) # Create an empty dataframe with the required columns
    tables = data.find_all("tbody") # Locate the table based on the table_attribs
    rows = tables[2].find_all("tr") # Extract rows with hyperlinks and required columns
    for row in rows:# Iterate through each row in the table
        col = row.find_all("td") # Find all the columns in the row
        if len(col) != 0: # Check if the row is not empty
            if col[0].find("a") is not None and '-' not in col[2]:
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
    GDP_list = df["GDP_USD_millions"].tolist()  # Convert the GDP column to a list
    # Handle special characters like '—' and convert to float
    GDP_list = [float("".join(x.split(','))) if x != '—' else 0.0 for x in GDP_list]
    GDP_list = [np.round(x/1000,2) for x in GDP_list]
    df["GDP_USD_millions"] = GDP_list
    df=df.rename(columns = {"GDP_USD_millions":"GDP_USD_billions"})
    return df

def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a `CSV` file 
    in the provided path. Function returns nothing.'''
    df.to_csv(csv_path, index=False)

sql_connection = mysql.connector.connect(
    host="localhost",
    database=db_name,
    user="root",
    password="chosandarhtet",
    port="3306",
)
print("Connected to MySQL database")

# Create a SQLAlchemy engine
engine = sqlalchemy.create_engine(
    "mysql+mysqlconnector://root:chosandarhtet@localhost:3306/world_economies"
)

# Check if the table exists, and create it if it doesn't
inspector = inspect(engine)
if not inspector.has_table(table_name):
    with engine.connect() as connection:
        create_table_query = f"""
        CREATE TABLE {table_name} (
            `Country` VARCHAR(255),
            `GDP_USD_billions` VARCHAR(255)
        );
        """
        connection.execute(create_table_query)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final dataframe as a database table
    with the provided name. Function returns nothing.'''
    # Create an SQLAlchemy engine for MySQL
    engine = sqlalchemy.create_engine(f'mysql+mysqlconnector://root:chosandarhtet@localhost:3306/{db_name}')
    df.to_sql(name=table_name, con=engine, if_exists="replace", index=False)
    print("Data has been successfully inserted into the database.")
    # Don't close the connection here

def run_query(query_statement, connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(query_statement)
    query_output = pd.read_sql(query_statement, connection)
    print(query_output)

def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the code execution to a log file. Function returns nothing'''

    timestamp_format = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(timestamp + " : " + message + "\n")

''' Here, you define the required entities and call the relevant 
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)

log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df)

log_progress('Data transformation complete. Initiating loading process')

load_to_csv(df, csv_path)

log_progress('Data saved to CSV file')

# Keep the MySQL connection
sql_connection = mysql.connector.connect(
    host="localhost",
    database=db_name,
    user="root",
    password="chosandarhtet",
    port="3306",
)

log_progress('SQL Connection initiated.')

load_to_db(df, sql_connection, table_name)

log_progress('Data loaded to Database as table. Running the query')

# Create a new connection for querying or use SQLAlchemy
engine = sqlalchemy.create_engine(f'mysql+mysqlconnector://root:chosandarhtet@localhost:3306/{db_name}')
query_statement = f"SELECT * from {table_name} WHERE GDP_USD_billions >= 100"
# Use the engine.connect() for the query
with engine.connect() as connection:
    run_query(query_statement, connection)

log_progress('Process Complete.')

sql_connection.close()

