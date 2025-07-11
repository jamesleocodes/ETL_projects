# ETL Project
This project demonstrates the process of extracting data from a source, transforming it to align with specific criteria, and loading it into a designated destination, such as a MySQL database. It also includes steps for setting up the MySQL database include installation, schema creation, connection configuration, and uploading transformed data into the tables. Additionally, it highlights the importance of incorporating unit tests to verify functionality at each stage of the pipeline and automated Continuous Integration (CI) workflows using GitHub Actions. The CI pipeline ensures reliability by automating tasks such as dependency installation, running tests, and validating changes whenever code is pushed or updated, streamlining development and maintaining high code quality. For those new to data engineering, a foundational article on the topic can be found [here](https://medium.com/@mr.zawmyowin.physics/data-engineering-simplified-techniques-tools-and-insights-for-aspiring-professionals-a8a4f29f78bb). This resource provides an excellent starting point for understanding the basics of data engineering.

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
## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.


## Contributing

Contributions are welcome! Please fork the repository and submit a pull request.

# Simple ETL Projects

This repository contains a collection of simple ETL (Extract, Transform, Load) projects that demonstrate different data processing techniques and tools.

## Projects

### Movies : ETL for movie data (`movies_project.py`)

#### Overview
This project extracts movie data from a web source, transforms it into a structured format, and loads it into both a CSV file and a MySQL database. The project demonstrates web scraping, data cleaning, and database operations.

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

### Storm Dynamics : ETL for atmospheric data (`phase1etl.py`)

#### Overview
This project implements an ETL pipeline for processing atmospheric data related to Severe Convective Storm (SCS) events. It extracts simulated atmospheric data, transforms it by calculating various indices and classifications, and loads the processed data into a SQLite database for analysis.

#### Features
- **Extraction**:
  - Simulates atmospheric data generation (CAPE, wind shear, temperature, humidity)
  - Saves raw data to CSV files in `data/raw` directory
  - Configurable date ranges and data parameters

- **Transformation**:
  - Calculates SCS Index using normalized CAPE and wind shear values
  - Classifies storm events based on atmospheric conditions
  - Adds seasonal information to the data
  - Performs data validation and quality checks
  - Saves processed data to CSV files in `data/processed` directory

- **Loading**:
  - Creates and manages SQLite database
  - Implements efficient data loading with batch processing
  - Creates necessary indexes for optimized queries
  - Includes data versioning and tracking

- **Configuration**:
  - Uses `config.json` for dynamic configuration
  - Configurable parameters for:
    - Database settings
    - Date ranges
    - Data retention policies
    - Batch processing sizes
    - Logging levels

- **Additional Features**:
  - Comprehensive logging system
  - Error handling and recovery
  - Data verification and validation
  - Progress tracking
  - Integration with analysis pipeline

#### Usage
1. Configure the ETL process in `config.json`
2. Run the ETL pipeline:
   ```bash
   python phase1etl.py
   ```
3. The processed data will be available in:
   - SQLite database: `data/storm_data.db`
   - Processed CSV files: `data/processed/`
   - Log file: `etl_process.log`

#### Analysis Integration
The ETL pipeline is integrated with an analysis script (`phase1.py`) that:
- Loads processed data from the database
- Performs attribution analysis
- Generates visualizations
- Produces statistical insights

## Requirements
- Python 3.9+
- Required packages listed in `requirements.txt`
- SQLite3 for database operations
- Configuration files for each project

## Installation
1. Clone the repository
2. Install required packages:
   ```bash
   pip install -r requirements.txt
   ```
3. Configure the projects using their respective configuration files

## Contributing
Contributions are welcome! Please feel free to submit a Pull Request.

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
