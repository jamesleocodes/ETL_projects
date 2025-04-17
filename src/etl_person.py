"""ETL process for processing person data from different file formats.

This module extracts person data from CSV, JSON, and XML files, transforms height and weight
data from imperial to metric units, and loads the results to a target CSV file.
"""

# Load the necessary libraries
import glob
import xml.etree.ElementTree as ET
from datetime import datetime
import pandas as pd
import yaml  # Import PyYAML for reading config files


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
            height = float(person.find("height").text)  # Convert to float
            weight = float(person.find("weight").text)  # Convert to float
            data.append({"name": name, "height": height, "weight": weight})
        except ValueError as ve:
            print(f"Skipping invalid row: {ve}")
            continue
        except AttributeError as ae:
            print(f"Skipping invalid row: Missing element - {ae}")
            continue
    # Create DataFrame
    dataframe = pd.DataFrame(data)
    if not dataframe.empty:
        dataframe = dataframe.astype(
            {"name": "object", "height": "float64", "weight": "float64"}
        )
    return dataframe


# write a function to call the respective function based on the file type
def extract():
    """Extract data from CSV, JSON, and XML files in the data folder.

    Returns:
        pandas.DataFrame: Combined data from all processed files with name,
                         height, and weight columns
    """
    data_frame = pd.DataFrame(columns=["name", "height", "weight"])

    # process all csv files in the data folder, except the target file
    for csvfile in glob.glob(f"{data_folder}/*.csv"):
        if csvfile != target_file:
            data_frame = pd.concat(
                [data_frame, extract_from_csv(csvfile)], ignore_index=True
            )

    # process all json files in the data folder
    for jsonfile in glob.glob(f"{data_folder}/*.json"):
        data_frame = pd.concat(
            [data_frame, extract_from_json(jsonfile)], ignore_index=True
        )

    # process all xml files in the data folder
    for xmlfile in glob.glob(f"{data_folder}/*.xml"):
        data_frame = pd.concat(
            [data_frame, extract_from_xml(xmlfile)], ignore_index=True
        )

    return data_frame


# Transform the data
def transform(data):
    """Convert inches to meters and round off to two decimals 1 inch = 0.0254 meters
    Convert pounds to kilograms and round off to two decimals 1 pound = 0.453592 kg
    """
    # Convert height from inches to meters
    data["height"] = (data["height"].astype(float) * 0.0254).round(2)
    # Convert weight from pounds to kilograms
    data["weight"] = (data["weight"].astype(float) * 0.453592).round(2)
    return data


# Load the data into a target file
def load_data(output_path, data_frame):
    """Load the data into a target file

    Args:
        output_path (str): Path to the output CSV file
        data_frame (pandas.DataFrame): Transformed data to be saved
    """
    data_frame.to_csv(output_path, index=False)


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
load_data(output_path=target_file, data_frame=transformed_data)

# Log the completion of the Loading process
log_progress("Load phase Ended", log_file)

# Log the completion of the ETL process
log_progress("ETL Job Ended", log_file)
