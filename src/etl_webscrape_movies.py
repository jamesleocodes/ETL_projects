"""ETL process for web scraping movie data from online sources.

This module extracts top 50 movie data from a specified web source, transforms it
and loads the results to a CSV file and a MySQL database.
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import yaml
import mysql.connector
import sqlalchemy
from sqlalchemy import inspect
from datetime import datetime


# Define a log_progress function directly in this file
def log_progress(message, log_file):
    """Log the message with timestamp to the specified log file.

    Args:
        message (str): Message to log
        log_file (str): Path to the log file
    """
    timestamp_format = "%Y-%h-%d-%H:%M:%S"  # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(f"{timestamp} - {message}\n")


# Load configuration from config.yaml
with open("../config.yaml", "r", encoding="utf-8") as stream:
    config = yaml.safe_load(stream)

# Get paths dynamically from config.yaml
URL = config["etl_webscrape_movies"]["source"]["url"]
DB_NAME = config["etl_webscrape_movies"]["source"]["db_name"]
TABLE_NAME = config["etl_webscrape_movies"]["source"]["table_name"]
CSV_PATH = config["etl_webscrape_movies"]["source"]["location"]

# Initialize the known entities
df = pd.DataFrame(columns=["Average Rank", "Film", "Year"])
MOVIE_COUNT = 0

# Loading the webpage for webscraping
html_page = requests.get(URL, timeout=10)
data = BeautifulSoup(html_page.text, "html.parser")

# Scraping the required information
tables = data.find_all("tbody")
rows = tables[0].find_all("tr")
for row in rows:
    if MOVIE_COUNT < 50:
        col = row.find_all("td")
        if len(col) != 0:
            data_dict = {
                "Average Rank": col[0].text.strip(),
                "Film": col[1].text.strip(),
                "Year": col[2].text.strip(),
            }
            df1 = pd.DataFrame([data_dict])
            df = pd.concat([df, df1], ignore_index=True)
            MOVIE_COUNT += 1

    else:
        break
print(df)
df.to_csv(CSV_PATH, index=False)

# Connect to MySQL
conn = mysql.connector.connect(
    host="localhost",
    database=DB_NAME,
    user="root",
    password="chosandarhtet",
    port="3306",
)
print("Connected to MySQL database")

# Create a SQLAlchemy engine
engine = sqlalchemy.create_engine(
    f"mysql+mysqlconnector://root:chosandarhtet@localhost:3306/{DB_NAME}"
)

# Check if the table exists, and create it if it doesn't
inspector = inspect(engine)
if not inspector.has_table(TABLE_NAME):
    with engine.connect() as connection:
        COLUMNS_DEF = (
            "`Average Rank` VARCHAR(255), "
            "`Film` VARCHAR(255), "
            "`Year` VARCHAR(255)"
        )
        create_table_query = f"CREATE TABLE {TABLE_NAME} ({COLUMNS_DEF});"
        connection.execute(create_table_query)

# Write the DataFrame to the table
df.to_sql(name=TABLE_NAME, con=engine, if_exists="replace", index=False)
conn.close()
print("Data has been successfully inserted into the database.")
