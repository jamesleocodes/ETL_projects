# ETL Project

This project is a basic implementation of an ETL (Extract, Transform, Load) pipeline. It demonstrates how to extract data from a source, transform it to meet specific requirements, and load it into a target destination.

## Features

- **Extract**: Retrieve data from various sources (e.g., CSV, JSON, APIs).
- **Transform**: Clean, filter, and manipulate data to fit the desired format.
- **Load**: Store the processed data into a database or file.

## Unit Test
The Simple ETL Project test suite validates the extract, transform, and load (ETL) pipeline for processing car and person data, ensuring robust and reliable data handling.


### test_etl_car.py
- **Extract**: Validates data extraction from CSV, JSON, and XML formats, with specific handling for missing `car_model` fields.
- **Transform**: Rounds prices to two decimal places and replaces missing `car_model` values with "Unknown".
- **Load**: Ensures the transformed data is correctly saved to a CSV file.

### test_etl_person.py
- **Extract**: Validates data extraction from CSV, JSON, and XML formats.
- **Transform**: Converts height to meters and weight to kilograms.
- **Load**: Verifies that the transformed data is saved to a CSV file.

## Continuous Integration (CI) pipeline

The CI pipeline is automated using GitHub Actions (`.github/workflows/ci.yaml`) and performs the following steps:
- Sets up a Python 3.10 environment.
- Installs dependencies, including `pytest` and `pylint`.
- Runs linting with `pylint` to enforce code quality.
- Executes tests for both `test_etl_car.py` and `test_etl_person.py` suites.

The CI pipeline runs on every push or pull request, ensuring the reliability and consistency of the ETL pipeline.

## Requirements

- Python 3.8+
- Required libraries are listed in `requirements.txt`.
- Ensure MySQL is already set up and running locally on your system. Please see how to set up mysql in your system in setupSQL.md file.

## Installation

1. Clone the repository:
  ```bash
  git clone https://github.com/jamesleocodes/ETL_projects.git
  ```

2. Install dependencies:
  ```bash
  pip install -r requirements.txt
  ```

## Usage

1. Configure the ETL pipeline by editing the `config.yaml` file.
2. Run the ETL script:
  ```bash
  python etl_person.py
  python etl_car.py
  ```

### Person: ETL for Person Data (`etl_person.py`)

#### Overview
This project processes person-related data, extracting fields such as `name`, `height`, and `weight`. The pipeline converts height from inches to meters and weight from pounds to kilograms.

#### Features
- **Extraction**: Reads data from CSV, JSON, and XML files in the `data` directory.
- **Transformation**: Converts:
  - `height` from inches to meters (rounded to 2 decimal points).
  - `weight` from pounds to kilograms (rounded to 2 decimal points).
- **Loading**: Saves the transformed data into `data/transformed_data.csv`.
- **Logging**: Logs each phase of the ETL process to `data/log_file.txt`.

#### How to Run
1. Place your input files (`.csv`, `.json`, `.xml`) in the `data` directory.
2. Run the ETL script:
   ```bash
   python3 etl_person.py
   ```
3. Check the output:
   - Transformed data: `data/transformed_data.csv`
   - Logs: `data/log_file.txt`

---

### Car: ETL for Car Data (`etl_car.py`)

#### Overview
This project processes car-related data, extracting fields such as `year_of_manufacture`, `price`, and `fuel`. The pipeline ensures that the `price` field is transformed into a consistent format with two decimal points.

#### Features
- **Extraction**: Reads data from CSV, JSON, and XML files in the `data_car` directory.
- **Transformation**: Converts the `price` field to two decimal points.
- **Loading**: Saves the transformed data into `data_car/transformed_data.csv`.
- **Logging**: Logs each phase of the ETL process to `data_car/log_file.txt`.

#### How to Run
1. Place input files (`.csv`, `.json`, `.xml`) in the `data_car` directory.
2. Run the script:
   ```bash
   python3 etl_car.py
   ```
3. Check the output:
   - Transformed data: `data_car/transformed_data.csv`
   - Logs: `data_car/log_file.txt`

### Movies: ETL for Movies Data (`etl_webscrape_movies.py`)

#### Overview
This project processes movie-related data by scraping information from a webpage. It extracts fields such as `Average Rank`, `Film`, and `Year`, and saves the data into a CSV file and a MySQL database.

#### Features
- **Extraction**: Scrapes movie data from a webpage using `BeautifulSoup`.
- **Transformation**: Cleans and structures the scraped data into a tabular format.
- **Loading**: 
  - Saves the transformed data into a CSV file.
  - Inserts the data into a MySQL database table.
- **Configuration**: Dynamically loads configurations (e.g., URL, database name, table name, and file paths) from a `config.yaml` file.

### Bank : ETL for bank data (`banks_project.py`)

#### Overview
This bank project extracts data about the largest banks by market capitalization from a web source. The data is transformed by converting market cap values from USD to other currencies like GBP, EUR, and INR. Transformed data is loaded into both a CSV file and a MySQL database for storage and analysis. The project includes logging capabilities to track progress throughout the ETL pipeline.

#### Features
- **Extraction**: Scrapes bank data from Wikipedia using `BeautifulSoup` and `pandas`.
- **Transformation**: Converts market cap values from USD to GBP, EUR, and INR currencies.
- **Loading**: 
  - Saves the transformed data into a CSV file.
  - Inserts the data into a MySQL database table.
- **Logging**: Tracks progress at each stage of the ETL process.
- **Configuration**: Dynamically loads configurations (URL, database credentials, table names, and file paths) from a `config.yaml` file.

#### How to Run
1. Ensure the `config.yaml` file is correctly configured with the following keys:
   ```yaml
   etl_webscrape_movies:
    source:
      type: web
      url: https://web.archive.org/web/20230902185655/https://en.everybodywiki.com/100_Most_Highly-Ranked_Films
      db_name: films
      table_name: Top_50
      location: ../data_scrapeMovies/top_50_films.csv
   ```
2. Run the script:
  Navigate to the src folder and run the desired project(python file) by entering the following command in the terminal:
   ```bash
   python3 etl_webscrape_movies.py
   ```
3. Check the output:
   - Transformed data: `data_scrapeMovies/top_50_films.csv`
   - MySQL database: Data inserted into the `Top_50` table in the `films` database.

#### Requirements
- **Python Libraries**:
  - `requests`
  - `pandas`
  - `BeautifulSoup4`
  - `PyYAML`
  - `SQLAlchemy`
  - `mysql-connector-python`
- **MySQL Database**: Ensure a MySQL server is running locally or remotely.

#### Example Output
- **CSV File**:
  ```
  Average Rank,Film,Year
  1,The Shawshank Redemption,1994
  2,The Godfather,1972
  ...
  ```
- **MySQL Table**:
  ```
  +---------------+---------------------------+------+
  | Average Rank  | Film                      | Year |
  +---------------+---------------------------+------+
  | 1             | The Shawshank Redemption  | 1994 |
  | 2             | The Godfather             | 1972 |
  +---------------+---------------------------+------+
  ```
---

## Project Structure

```
etl/
├── data/
│   ├── person/                     # Data files for person data
│   ├── car/                        # Data files for car data
│   ├── movies/                     # Data files for movie data
│   ├── gdp/                        # Data files for gdp by countries
│   └── bank/                       # Data files for exchange rate
├── output/                         # Processed data (e.g., CSVs, SQLite DBs)
├── src/                            # ETL scripts (renamed from scripts/)
│   ├── etl_person.py               # ETL script for person data (renamed from etl_test.py)
│   ├── etl_car.py                  # ETL script for car data
│   ├── etl_webscrape_movies.py     # ETL script for movie data 
│   ├── etl_gdp.py                  # ETL script for gdp scraping
│   └── banks_project.py            # ETL script for bank information scraping
├── tests/
│   ├── test_etl_person.py          # Unit test for etl_person.py
│   └──  test_etl_car.py            # Unit test for etl_car.py
├── config.yaml                     # Configuration file
├── requirements.txt                # Python dependencies
├── .gitignore                      # Git ignore file
└── README.md                       # Project documentation

```

## Contributing

Contributions are welcome! Please fork the repository and submit a pull request.
