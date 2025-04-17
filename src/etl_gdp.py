"""ETL process for extracting GDP data from web sources.

This module extracts GDP data from a specified website, transforms it by converting
GDP values from millions to billions USD, and loads the results to a CSV file and database.
"""

from datetime import datetime

import mysql.connector
import numpy as np
import pandas as pd
import requests
import sqlalchemy
import yaml
from bs4 import BeautifulSoup
from sqlalchemy import inspect

# Load configuration from config.yaml
with open("../config.yaml", "r", encoding="utf-8") as stream:
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


def extract(source_url, attrs):
    """This function extracts the required
    information from the website and saves it to a dataframe. The
    function returns the dataframe for further processing."""

    page = requests.get(source_url, timeout=10)  # Fetch the webpage
    data = BeautifulSoup(page.text, "html.parser")  # Parse the webpage
    result_df = pd.DataFrame(
        columns=attrs
    )  # Create an empty dataframe with the required columns
    # Locate the table based on the table_attribs
    tables = data.find_all("tbody")
    # Extract rows with hyperlinks and required columns
    rows = tables[2].find_all("tr")
    for row in rows:  # Iterate through each row in the table
        col = row.find_all("td")  # Find all the columns in the row
        if len(col) != 0:  # Check if the row is not empty
            if col[0].find("a") is not None and "-" not in col[2]:
                data_dict = {
                    "Country": col[0].a.contents[0],
                    "GDP_USD_millions": col[2].contents[0],
                }
                df1 = pd.DataFrame(data_dict, index=[0])
                result_df = pd.concat([result_df, df1], ignore_index=True)
    return result_df


def transform(data_frame):
    """This function converts the GDP information from Currency
    format to float value, transforms the information of GDP from
    USD (Millions) to USD (Billions) rounding to 2 decimal places.
    The function returns the transformed dataframe."""
    gdp_list = data_frame[
        "GDP_USD_millions"
    ].tolist()  # Convert the GDP column to a list
    # Handle special characters like '—' and convert to float
    gdp_list = [float("".join(x.split(","))) if x != "—" else 0.0 for x in gdp_list]
    gdp_list = [np.round(x / 1000, 2) for x in gdp_list]
    data_frame["GDP_USD_millions"] = gdp_list
    data_frame = data_frame.rename(columns={"GDP_USD_millions": "GDP_USD_billions"})
    return data_frame


def load_to_csv(data_frame, output_path):
    """This function saves the final dataframe as a `CSV` file
    in the provided path. Function returns nothing."""
    data_frame.to_csv(output_path, index=False)


# Connect to MySQL database
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
    f"mysql+mysqlconnector://root:chosandarhtet@localhost:3306/{db_name}"
)

# Check if the table exists and create it if needed
inspector = inspect(engine)
if not inspector.has_table(table_name):
    with engine.connect() as connection:
        TABLE_COLUMNS = "`Country` VARCHAR(255), `GDP_USD_billions` VARCHAR(255)"
        create_table_query = f"""
        CREATE TABLE {table_name} (
            {TABLE_COLUMNS}
        );
        """
        connection.execute(create_table_query)


def load_to_db(data_frame, db_table_name):
    """This function saves the final dataframe as a database table
    with the provided name. Function returns nothing."""
    # Create an SQLAlchemy engine for MySQL
    db_engine = sqlalchemy.create_engine(
        f"mysql+mysqlconnector://root:chosandarhtet@localhost:3306/{db_name}"
    )
    data_frame.to_sql(
        name=db_table_name, con=db_engine, if_exists="replace", index=False
    )
    print("Data has been successfully inserted into the database.")


def run_query(sql_stmt, sql_conn):
    """This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing."""
    print(sql_stmt)
    query_output = pd.read_sql(sql_stmt, sql_conn)
    print(query_output)


def log_progress(message):
    """This function logs the mentioned message at a given stage of the code execution
    to a log file. Function returns nothing"""

    timestamp_format = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(timestamp + " : " + message + "\n")


log_progress("Preliminaries complete. Initiating ETL process")

df = extract(source_url=url, attrs=table_attribs)

log_progress("Data extraction complete. Initiating Transformation process")

df = transform(data_frame=df)

log_progress("Data transformation complete. Initiating loading process")

load_to_csv(data_frame=df, output_path=csv_path)

log_progress("Data saved to CSV file")

log_progress("SQL Connection initiated.")

# MySQL connection already established in the connect_to_mysql call
load_to_db(data_frame=df, db_table_name=table_name)

log_progress("Data loaded to Database as table. Running the query")

# Create a new connection for querying or use SQLAlchemy
query_engine = sqlalchemy.create_engine(
    f"mysql+mysqlconnector://root:chosandarhtet@localhost:3306/{db_name}"
)
sql_query = f"SELECT * from {table_name} WHERE GDP_USD_billions >= 100"
# Use the engine.connect() for the query
with query_engine.connect() as conn:
    run_query(sql_stmt=sql_query, sql_conn=conn)

log_progress("Process Complete.")

sql_connection.close()
