# Load the necessary libraries
import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

# Introduce the file path
log_file = "log_file.txt"
target_file = "transformed_data.csv"

# Develop functions to extract data from different formats
# from csv
def extract_from_csv(file_to_process):
    """Extract data from csv file"""
    dataframe = pd.read_csv(file_to_process)
    return dataframe

# from json
def extract_from_json(file_to_process):
    """Extract data from json file"""
    dataframe = pd.read_json(file_to_process)
    return dataframe

# from xml
def extract_from_xml(file_to_process):
    """Extract data from xml file"""
    dataframe = pd.DataFrame(columns = ['name','height','weight'])
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    for person in root:
        name = person.find('name').text
        height = person.find('height').text
        weight = person.find('weight').text
        dataframe = dataframe.append({'name': name, 'height': height, 'weight': weight}, ignore_index=True)
    return dataframe

# write a function to call the respective function based on the file type
def extract():
    extracted_data = pd.DataFrame(columns =['name','height','weight'])

    # process all csv files, except the target file
    for csvfile in glob.glob("*.csv"):
        if csvfile != target_file:
            extracted_data = pd.concat([extracted_data, extract_from_csv(csvfile)], ignore_index=True)
    
    # process all json files
    for jsonfile in glob.glob("*.json"):
        extracted_data = pd.concat([extracted_data, extract_from_json(jsonfile)], ignore_index=True)
    
    # process all xml files
    for xmlfile in glob.glob("*.xml"):
        extracted_data = pd.concat([extracted_data, extract_from_xml(xmlfile)], ignore_index=True)
    
    return extracted_data

# Transform the data
def transform(data):
    """Convert inches to meters and round off to two decimals 1 inch = 0.0254 meters"""
    data['height'] = (data['height'].astype(float) * 0.0254).round(2)
    data['weight'] = (data['weight'].astype(float) * 0.453592).round(2)
    return data

# Load the data into a target file
def load_data(target_file, transformed_data):
    """Load the data into a target file"""
    transformed_data.to_csv(target_file, index=False)

  
# Log the process
def log_process(message):
    """Log the process"""
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, 'a') as f:
        f.write(f"{timestamp} - {message}\n")