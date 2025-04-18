"""ETL process for processing car data from different file formats.

This module extracts car data from CSV, JSON, and XML files, transforms the price data,
and loads the results to a target CSV file. The process is logged.
"""

# Standard library imports
import glob
import os
import sys
import xml.etree.ElementTree as ET

# Third-party library imports
import pandas as pd
import yaml

# Add the 'src' directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Local imports
from src.utils import log_progress

# Load configuration from config.yaml
config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../config.yaml")
with open(config_path, "r", encoding="utf-8") as stream:
    config = yaml.safe_load(stream)

# Get paths dynamically from config.yaml
# log_file = config["etl_car"]["logging"]["location"]
base_path = os.path.abspath(os.getcwd())
log_file = os.path.join(base_path, "output/log_file_car.txt")
target_file = config["etl_car"]["target"]["location"]
data_folder = config["etl_car"]["source"]["location"]


# Develop functions to extract data from different formats
# from csv
def extract_from_csv(file_to_process):
    """Extract data from csv file"""
    dataframe = pd.read_csv(file_to_process)
    return dataframe


# from json
def extract_from_json(file_to_process):
    """Extract data from json file"""
    dataframe = pd.read_json(file_to_process, lines=True)
    return dataframe


# from xml
def extract_from_xml(file_to_process):
    """Extract data from XML file."""
    # Initialize an empty list to collect rows
    data = []
    # Parse the XML file
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    # Iterate through each car in the XML
    for car in root:
        try:
            year_of_manufacture = int(car.find("year_of_manufacture").text)
            price = float(car.find("price").text)
            fuel = car.find("fuel").text
            data.append(
                {
                    "year_of_manufacture": year_of_manufacture,
                    "price": price,
                    "fuel": fuel,
                }
            )
        except (ValueError, AttributeError) as e:
            # Skip rows with invalid or missing data
            print(f"Skipping invalid row: {e}")
            continue
    # Create DataFrame and enforce dtypes
    dataframe = pd.DataFrame(data)
    if not dataframe.empty:
        dataframe = dataframe.astype(
            {"year_of_manufacture": "int64", "price": "float64", "fuel": "object"}
        )
    return dataframe


# write a function to call the respective function based on the file type
# Function to handle extraction based on file type
def extract():
    """Extract data from CSV, JSON, and XML files in the data folder.

    Returns:
        pandas.DataFrame: Combined data from all processed files
    """
    data_frame = pd.DataFrame(columns=["year_of_manufacture", "price", "fuel"])

    # Process all csv files in the data folder
    for csvfile in glob.glob(f"{data_folder}/*.csv"):
        if csvfile != target_file:
            data_frame = pd.concat(
                [data_frame, extract_from_csv(csvfile)], ignore_index=True
            )

    # Process all json files in the data folder
    for jsonfile in glob.glob(f"{data_folder}/*.json"):
        data_frame = pd.concat(
            [data_frame, extract_from_json(jsonfile)], ignore_index=True
        )

    # Process all xml files in the data folder
    for xmlfile in glob.glob(f"{data_folder}/*.xml"):
        data_frame = pd.concat(
            [data_frame, extract_from_xml(xmlfile)], ignore_index=True
        )

    return data_frame


# Transform the data
def transform(data):
    """Transform price into two decimal points"""
    data["price"] = (data["price"].astype(float)).round(2)
    return data


# Load the data into a target file
def load_data(out_path, data_frame):
    """Load the data into a target file

    Args:
        out_path (str): Path to the output file
        data_frame (pandas.DataFrame): Data to be saved
    """
    data_frame.to_csv(out_path, index=False)


# Log the initialization of the ETL process
log_progress("ETL Job Started", log_file)

# Log the beginning of the Extraction process
log_progress("Extract phase Started", log_file)
extracted_data = extract()

# Log the completion of the Extraction process
log_progress("Extract phase Ended", log_file)

# Log the beginning of the Transformation process
log_progress("Transform phase Started", log_file)
transformed_data = transform(extracted_data)
print("Transformed Data")
print(transformed_data)

# Log the completion of the Transformation process
log_progress("Transform phase Ended", log_file)

# Log the beginning of the Loading process
log_progress("Load phase Started", log_file)
load_data(out_path=target_file, data_frame=transformed_data)

# Log the completion of the Loading process
log_progress("Load phase Ended", log_file)

# Log the completion of the ETL process
log_progress("ETL Job Ended", log_file)
