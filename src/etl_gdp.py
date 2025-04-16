import requests
import mysql.connector
import pandas as pd
from bs4 import BeautifulSoup
import yaml
import sqlalchemy
from sqlalchemy import inspect

# Load configuration from config.yaml
with open("../config.yaml", "r") as stream:
    config = yaml.safe_load(stream)

# Get paths dynamically from config.yaml
url = config["etl_gdp"]["source"]["url"]
db_name = config["etl_gdp"]["source"]["db_name"]
table_name = config["etl_gdp"]["source"]["table_name"]
csv_path = config["etl_gdp"]["source"]["location"]

# Initialize the known entities
df = pd.DataFrame(columns=["Country", "GDP (US$)", "Population", "Area (km²)"])
count = 0

def extract(url, table_attribs):
    ''' This function extracts the required
    information from the website and saves it to a dataframe. The
    function returns the dataframe for further processing. '''
    try:
        # Fetch the wepage as text
        response = requests.get(url)
        response.raise_for_status()
        webpage_text = response.text

        # Parse the webpage
        soup = BeautifulSoup(webpage_text, 'html.parser')

        # Locate the table based on the table_attribs
        tables = soup.find_all("table", attrs = table_attribs)[2]

        # Extract rows with hyperlinks and required columns
        extracted_data = []
        rows = tables.find_all("tr")
        for row in rows:
            cols = row.find_all("td")
            

    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    tables = soup.find_all("tbody")
    df = pd.read_html(str(tables))[0]
    return df
def transform(df):
    ''' This function converts the GDP information from Currency
    format to float value, transforms the information of GDP from
    USD (Millions) to USD (Billions) rounding to 2 decimal places.
    The function returns the transformed dataframe.'''
    return df
def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a `CSV` file 
    in the provided path. Function returns nothing.'''
def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final dataframe as a database table
    with the provided name. Function returns nothing.'''
def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''
def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the code execution to a log file. Function returns nothing'''
''' Here, you define the required entities and call the relevant 
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''
