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
url = config["etl_webscrape_movies"]["source"]["url"]
db_name = config["etl_webscrape_movies"]["source"]["db_name"]
table_name = config["etl_webscrape_movies"]["source"]["table_name"]
csv_path = config["etl_webscrape_movies"]["source"]["location"]

# Initialize the known entities
df = pd.DataFrame(columns=["Average Rank", "Film", "Year"])
count = 0

# Loading the webpage for webscraping
html_page = requests.get(url)
data = BeautifulSoup(html_page.text, "html.parser")

# Scraping the required information
tables = data.find_all("tbody")
rows = tables[0].find_all("tr")
for row in rows:
    if count < 50:
        col = row.find_all("td")
        if len(col) != 0:
            data_dict = {
                "Average Rank": col[0].text.strip(),
                "Film": col[1].text.strip(),
                "Year": col[2].text.strip(),
            }
            df1 = pd.DataFrame([data_dict])
            df = pd.concat([df, df1], ignore_index=True)
            count += 1

    else:
        break
print(df)
df.to_csv(csv_path, index=False)


conn = mysql.connector.connect(
    host="localhost",
    database=db_name,
    user="root",
    password="password",
    port="3306",
)
print("Connected to MySQL database")

# Create a SQLAlchemy engine
engine = sqlalchemy.create_engine(
    "mysql+mysqlconnector://root:password@localhost:3306/films"
)

# Check if the table exists, and create it if it doesn't
inspector = inspect(engine)
if not inspector.has_table(table_name):
    with engine.connect() as connection:
        create_table_query = f"""
        CREATE TABLE {table_name} (
          `Average Rank` VARCHAR(255),
          `Film` VARCHAR(255),
          `Year` VARCHAR(255)
        );
        """
        connection.execute(create_table_query)

# Write the DataFrame to the table
df.to_sql(name=table_name, con=engine, if_exists="replace", index=False)
conn.close()
print("Data has been successfully inserted into the database.")
