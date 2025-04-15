# ETL Project

This project is a basic implementation of an ETL (Extract, Transform, Load) pipeline. It demonstrates how to extract data from a source, transform it to meet specific requirements, and load it into a target destination.

## Features

- **Extract**: Retrieve data from various sources (e.g., CSV, JSON, APIs).
- **Transform**: Clean, filter, and manipulate data to fit the desired format.
- **Load**: Store the processed data into a database or file.

## Requirements

- Python 3.8+
- Required libraries are listed in `requirements.txt`.

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
  python etl_test.py
  python etl_car.py
  ```

### Person: ETL for Person Data (`etl_test.py`)

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
   python3 etl_test.py
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

---

## Project Structure

```
etl/
├── data/               # Sample data files for person data
├── data_car/           # Sample data files for car data
├── scripts/            # ETL scripts
├── tests/              # Unit tests
├── config.yaml         # Configuration file
├── requirements.txt    # Python dependencies
├── README.md           # Project documentation
├── etl_test.py         # ETL script for person data
└── etl_car.py          # ETL script for car data
```

## Contributing

Contributions are welcome! Please fork the repository and submit a pull request.
