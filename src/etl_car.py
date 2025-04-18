"""
ETL process for processing car data from different file formats.

This module extracts car data from CSV, JSON, and XML files, transforms the price data,
and loads the results to a target CSV file. The process is logged.
"""

# Standard library imports
import glob
import os
import sys
import xml.etree.ElementTree as ET
from typing import List

# Third-party library imports
import pandas as pd
import yaml
from pandas import DataFrame

# Add the 'src' directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.utils import log_progress

# Load configuration from config.yaml
config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../config.yaml")
with open(config_path, "r", encoding="utf-8") as stream:
    config = yaml.safe_load(stream)

# Get paths dynamically from config.yaml
base_path = os.path.abspath(os.path.dirname(__file__))
log_file = os.path.join(base_path, config["etl_car"]["logging"]["location"])
target_file = os.path.join(base_path, "../output/car_data.csv")
data_folder = os.path.join(base_path, "../data_car")

# Ensure directories exist
os.makedirs(os.path.dirname(log_file), exist_ok=True)
os.makedirs(os.path.dirname(target_file), exist_ok=True)
if not os.path.exists(data_folder):
    raise FileNotFoundError(f"Data folder {data_folder} does not exist.")

# # Logging function
# def log_progress(message: str, log_file: str) -> None:
#     """Log a message to the specified log file."""
#     with open(log_file, "a", encoding="utf-8") as f:
#         f.write(f"{message}\n")

# Extraction functions
def extract_from_csv(file_to_process: str) -> pd.DataFrame:
    """Extract car data from a CSV file."""
    try:
        dataframe = pd.read_csv(file_to_process)
        # Ensure required columns exist
        required_columns = ["year_of_manufacture", "price", "fuel", "car_model"]
        for col in required_columns:
            if col not in dataframe.columns:
                dataframe[col] = None
        dataframe = dataframe[required_columns].astype(
            {
                "year_of_manufacture": "int64",
                "price": "float64",
                "fuel": "object",
                "car_model": "object",
            }
        )
        return dataframe
    except Exception as e:
        print(f"Error processing {file_to_process}: {e}")
        return pd.DataFrame(
            columns=["year_of_manufacture", "price", "fuel", "car_model"]
        )

def extract_from_json(file_to_process: str) -> pd.DataFrame:
    """Extract car data from a JSON file."""
    try:
        dataframe = pd.read_json(file_to_process, lines=True)
        # Ensure required columns exist
        required_columns = ["year_of_manufacture", "price", "fuel", "car_model"]
        for col in required_columns:
            if col not in dataframe.columns:
                dataframe[col] = None
        dataframe = dataframe[required_columns].astype(
            {
                "year_of_manufacture": "int64",
                "price": "float64",
                "fuel": "object",
                "car_model": "object",
            }
        )
        return dataframe
    except Exception as e:
        print(f"Error processing {file_to_process}: {e}")
        return pd.DataFrame(
            columns=["year_of_manufacture", "price", "fuel", "car_model"]
        )

def extract_from_xml(file_to_process: str) -> pd.DataFrame:
    """Extract car data from an XML file."""
    data = []
    try:
        tree = ET.parse(file_to_process)
        root = tree.getroot()
        for car in root:
            try:
                year_of_manufacture = int(car.find("year_of_manufacture").text)
                price = float(car.find("price").text)
                fuel = car.find("fuel").text
                car_model = (
                    car.find("car_model").text
                    if car.find("car_model") is not None
                    else None
                )
                data.append(
                    {
                        "year_of_manufacture": year_of_manufacture,
                        "price": price,
                        "fuel": fuel,
                        "car_model": car_model,
                    }
                )
            except (ValueError, AttributeError) as e:
                print(f"Skipping invalid row in {file_to_process}: {e}")
                continue
        dataframe = pd.DataFrame(data)
        if not dataframe.empty:
            dataframe = dataframe.astype(
                {
                    "year_of_manufacture": "int64",
                    "price": "float64",
                    "fuel": "object",
                    "car_model": "object",
                }
            )
        return dataframe
    except Exception as e:
        print(f"Error processing {file_to_process}: {e}")
        return pd.DataFrame(
            columns=["year_of_manufacture", "price", "fuel", "car_model"]
        )

def extract() -> pd.DataFrame:
    """Extract data from CSV, JSON, and XML files in the data folder."""
    data_frames = []
    columns = ["year_of_manufacture", "price", "fuel", "car_model"]
    files_processed = 0

    for csvfile in glob.glob(f"{data_folder}/*.csv"):
        if csvfile != target_file:
            df = extract_from_csv(csvfile)
            if df.empty or df["car_model"].isna().all():
                log_progress(f"Warning: No car_model data in {csvfile}", log_file)
            data_frames.append(df)
            files_processed += 1

    for jsonfile in glob.glob(f"{data_folder}/*.json"):
        df = extract_from_json(jsonfile)
        if df.empty or df["car_model"].isna().all():
            log_progress(f"Warning: No car_model data in {jsonfile}", log_file)
        data_frames.append(df)
        files_processed += 1

    for xmlfile in glob.glob(f"{data_folder}/*.xml"):
        df = extract_from_xml(xmlfile)
        if df.empty or df["car_model"].isna().all():
            log_progress(f"Warning: No car_model data in {xmlfile}", log_file)
        data_frames.append(df)
        files_processed += 1

    if files_processed == 0:
        log_progress("Warning: No files found to process.", log_file)
        return pd.DataFrame(columns=columns)

    data_frame = pd.concat(data_frames, ignore_index=True)
    if data_frame["car_model"].isna().any():
        log_progress(
            f"Warning: {data_frame['car_model'].isna().sum()} rows have missing car_model values.",
            log_file,
        )

    return data_frame

def transform(data: pd.DataFrame) -> pd.DataFrame:
    """Transform price into two decimal points and handle missing car_model."""
    data["price"] = data["price"].astype(float).round(2)
    data["car_model"] = data["car_model"].fillna("Unknown")
    return data

def load_data(output_path: str, data_frame: pd.DataFrame) -> None:
    """Load the data into a target CSV file."""
    data_frame.to_csv(output_path, index=False)

# Run ETL process
log_progress("Preliminaries complete. Initiating ETL process", log_file)
# Log the beginning of the Extraction process
# with open(log_file, "a", encoding="utf-8") as f:
#     f.write("\nExtract phase Started!\n")
extracted_data = extract()

log_progress("Data extraction complete. Initiating Transformation process", log_file)

# # Log the completion of the Extraction process
# with open(log_file, "a", encoding="utf-8") as f:
#     f.write("\nExtract phase Ended!\n")

# # Log the beginning of the Transformation process
# with open(log_file, "a", encoding="utf-8") as f:
#     f.write("\nTransform phase Started!\n")
transformed_data = transform(extracted_data)
print("Transformed Data")
print(transformed_data)
log_progress("Data transformation complete. Initiating loading process", log_file)

# # Log the completion of the Transformation process
# with open(log_file, "a", encoding="utf-8") as f:
#     f.write("\nTransform phase Ended!\n")

# # Log the beginning of the Loading process
# with open(log_file, "a", encoding="utf-8") as f:
#     f.write("\nLoad phase Started!\n")
load_data(output_path=target_file, data_frame=transformed_data)
log_progress("Data saved to CSV file", log_file)
log_progress("ETL Job Ended!", log_file)