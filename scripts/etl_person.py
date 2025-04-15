# Load the necessary libraries
import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

import yaml  # Import PyYAML for reading config files

# Load configuration from config.yaml
with open("../config.yaml", "r") as stream:
    config = yaml.safe_load(stream)

# Get paths dynamically from config.yaml
log_file = config["etl_person"]["logging"]["location"]
target_file = config["etl_person"]["target"]["location"]
data_folder = config["etl_person"]["source"]["location"]


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
    data = []
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    for person in root:
        try:
            name = person.find("name").text
            height = int(person.find("height").text)  # Convert to int
            weight = int(person.find("weight").text)  # Convert to int
            data.append({"name": name, "height": height, "weight": weight})
        except (ValueError, AttributeError) as e:
            print(f"Skipping invalid row: {e}")
            continue
    # Create DataFrame
    dataframe = pd.DataFrame(data)
    # Enforce dtypes if DataFrame is not empty
    if not dataframe.empty:
        dataframe = dataframe.astype(
            {"name": "object", "height": "int64", "weight": "int64"}
        )
    return dataframe


# write a function to call the respective function based on the file type
def extract():
    extracted_data = pd.DataFrame(columns=["name", "height", "weight"])

    # process all csv files in the data folder, except the target file
    for csvfile in glob.glob(f"{data_folder}/*.csv"):
        if csvfile != target_file:
            extracted_data = pd.concat(
                [extracted_data, extract_from_csv(csvfile)], ignore_index=True
            )

    # process all json files in the data folder
    for jsonfile in glob.glob(f"{data_folder}/*.json"):
        extracted_data = pd.concat(
            [extracted_data, extract_from_json(jsonfile)], ignore_index=True
        )

    # process all xml files in the data folder
    for xmlfile in glob.glob(f"{data_folder}/*.xml"):
        extracted_data = pd.concat(
            [extracted_data, extract_from_xml(xmlfile)], ignore_index=True
        )

    return extracted_data


# Transform the data
def transform(data):
    """Convert inches to meters and round off to two decimals 1 inch = 0.0254 meters"""
    data["height"] = (data["height"].astype(float) * 0.0254).round(2)
    data["weight"] = (data["weight"].astype(float) * 0.453592).round(2)
    return data


# Load the data into a target file
def load_data(target_file, transformed_data):
    """Load the data into a target file"""
    transformed_data.to_csv(target_file, index=False)


# Log the process
def log_progress(message):
    """Log the process"""
    timestamp_format = "%Y-%h-%d-%H:%M:%S"  # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(f"{timestamp} - {message}\n")


# Log the initialization of the ETL process
log_progress("ETL Job Started")

# Log the beginning of the Extraction process
log_progress("Extract phase Started")
extracted_data = extract()

# Log the completion of the Extraction process
log_progress("Extract phase Ended")

# Log the beginning of the Transformation process
log_progress("Transform phase Started")
transformed_data = transform(extracted_data)
print("Transformed Data")
print(transformed_data)

# Log the completion of the Transformation process
log_progress("Transform phase Ended")

# Log the beginning of the Loading process
log_progress("Load phase Started")
load_data(target_file, transformed_data)

# Log the completion of the Loading process
log_progress("Load phase Ended")

# Log the completion of the ETL process
log_progress("ETL Job Ended")
