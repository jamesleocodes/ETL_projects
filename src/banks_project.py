import requests
import numpy as np
from datetime import datetime
import mysql.connector
import pandas as pd
from bs4 import BeautifulSoup
import yaml
import sqlalchemy
from sqlalchemy import inspect

with open("../config.yaml", "r") as file:
    config = yaml.safe_load(file)

url = config["etl_bank"]["source"]["url"]
db_name = config["etl_bank"]["source"]["db_name"]
table_name = config["etl_bank"]["source"]["table_name"]
table_attribs = config["etl_bank"]["source"]["table_attribs"]
csv_file = config["etl_bank"]["source"]["location"]
code_log_file = config["etl_bank"]["logging"]["location"]
output_file = config["etl_bank"]["output"]["location"]

# Write a function log_progress() to log
# the progress of the code at different stages
# in a file code_log.txt. Use the list of log points
#  provided to create log entries as every stage of the code.


def log_progress(message):
    """This function logs the mentioned message at a given stage of t
    he code execution to a log file. Function returns nothing"""

    timestamp_format = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(code_log_file, "a") as f:
        f.write(timestamp + " : " + message + "\n")
    print(timestamp + " : " + message)


# Extract the tabular information from the given
# URL under the heading 'By market capitalization' and
# save it to a dataframe.
# a. Inspect the webpage and identify the position and pattern
# of the tabular information in the HTML code
# b. Write the code for a function extract() to perform the required data extraction.
# c. Execute a function call to extract() to verify the output.
# extract function
def extract(url, table_attrs):
    page = requests.get(url)
    soup = BeautifulSoup(page.text, "html.parser")

    # Find the table with bank data
    tables = soup.find_all("table")
    for i, table in enumerate(tables):
        if "Bank name" in table.text and "Market cap" in table.text:
            # Parse table directly to dataframe with pandas
            df = pd.read_html(str(table))[0]
            # Rename columns to match expected format
            df = df.rename(
                columns={
                    "Bank name": "Name",
                    "Market cap(US$ billion)": "MC_USD_Billion",
                }
            )
            return df

    print("Table not found")
    return pd.DataFrame()


# Transform the dataframe by adding columns for Market Capitalization in GBP,
# EUR and INR, rounded to 2 decimal places, based on the exchange rate information
# shared as a CSV file.
# a. Write the code for a function transform() to perform the said task.
# b. Execute a function call to transform() and verify the output.


# transform function
def transform(dataframe, csv_file):
    # Load exchange rates from CSV and convert to a dictionary
    exchange_rates = pd.read_csv(csv_file)
    rate_dict = dict(zip(exchange_rates["Currency"], exchange_rates["Rate"]))

    # Add transformed columns for Market Cap in GBP, EUR, INR (rounded to 2 decimals)
    dataframe["MC_GBP_Billion"] = [
        np.round(x * rate_dict["GBP"], 2) for x in dataframe["MC_USD_Billion"]
    ]
    dataframe["MC_EUR_Billion"] = [
        np.round(x * rate_dict["EUR"], 2) for x in dataframe["MC_USD_Billion"]
    ]
    dataframe["MC_INR_Billion"] = [
        np.round(x * rate_dict["INR"], 2) for x in dataframe["MC_USD_Billion"]
    ]
    print(dataframe)

    return dataframe


# Load the transformed dataframe to an output CSV file.
# rite a function load_to_csv(), execute a function call and verify the output.


# load to csv function
def load_to_csv(dataframe, output_file):
    dataframe.to_csv(output_file, index=False)
    print(f"Dataframe saved to {output_file}")


# load into sql database
sql_connection = mysql.connector.connect(
    host="localhost",
    database=db_name,
    user="root",
    password="password",
    port="3306",
)
print("Connected to MySQL database")

# Create a SQLAlchemy engine
engine = sqlalchemy.create_engine(
    "mysql+mysqlconnector://root:password@localhost:3306/" + db_name
)

# Check if the table exists, and create it if it doesn't
inspector = inspect(engine)
if not inspector.has_table(table_name):
    with engine.connect() as connection:
        create_table_query = f"""
        CREATE TABLE {table_name} (
            `Name` VARCHAR(255),
            `MC_USD_Billion` VARCHAR(255)
        );
        """
        connection.execute(create_table_query)


def load_to_db(df, sql_connection, table_name):
    """This function saves the final dataframe as a database table
    with the provided name. Function returns nothing."""
    # Create an SQLAlchemy engine for MySQL
    engine = sqlalchemy.create_engine(
        f"mysql+mysqlconnector://root:password@localhost:3306/{db_name}"
    )
    df.to_sql(name=table_name, con=engine, if_exists="replace", index=False)
    print("Data has been successfully inserted into the database.")


# Run queries on the database table.
# Write a function load_to_db(), execute a given set of queries and verify the output.
def run_query(query_statement, connection):
    """This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing."""
    print(query_statement)
    query_output = pd.read_sql(query_statement, connection)
    print(query_output)


""" Here, I  define the required entities and call the relevant 
functions in the correct order to complete the project. Note that this
portion is not inside any function."""

log_progress("Preliminaries complete. Initiating ETL process")

df = extract(url, table_attribs)
print(df)

log_progress("Data extraction complete. Initiating Transformation process")

df = transform(df, csv_file)

log_progress("Data transformation complete. Initiating loading process")

load_to_csv(df, output_file)

log_progress("Data saved to CSV file")

# Keep the MySQL connection
sql_connection = mysql.connector.connect(
    host="localhost",
    database=db_name,
    user="root",
    password="password",
    port="3306",
)
print("Connected to MySQL database")

load_to_db(df, sql_connection, table_name)

log_progress("Data loaded to Database as table. Running the query")

# Create a new connection for querying or use SQLAlchemy
engine = sqlalchemy.create_engine(
    f"mysql+mysqlconnector://root:password@localhost:3306/{db_name}"
)
query1 = "SELECT * FROM Largest_banks"

# Use the engine.connect() for the query
with engine.connect() as connection:
    run_query(query1, connection)

query2 = "SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
# Use the engine.connect() for the query
with engine.connect() as connection:
    run_query(query2, connection)

query3 = "SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
with engine.connect() as connection:
    run_query(query3, connection)

log_progress("Process Complete.")
sql_connection.close()
