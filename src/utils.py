# src/utils.py
"""Common utility functions for ETL processes."""

from datetime import datetime


# def log_progress(message, log_file):
#     """Log the message with timestamp to the specified log file.

#     Args:
#         message (str): Message to log
#         log_file (str): Path to the log file
#     """
#     timestamp_format = "%Y-%h-%d-%H:%M:%S"  # Year-Monthname-Day-Hour-Minute-Second
#     now = datetime.now()
#     timestamp = now.strftime(timestamp_format)
#     with open(log_file, "a", encoding="utf-8") as f:
#         f.write(timestamp + ":" + message + "\n")

def log_progress(message, log_file):
    """This function logs the mentioned message at a given stage of the code execution
    to a log file. Function returns nothing"""

    timestamp_format = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(timestamp + " : " + message + "\n")