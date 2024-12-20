#!/usr/bin/env python3
"""
Script Name: Ticker Tracker

Description:
    Grabs the NASDAQ historical data from https://www.nasdaq.com/
    for multiple tickers and combines the data into a .xlsx file.

Usage:
    1. Modify tickers.py to include all the tickers to scrape
    2. Run this Python script:
        python ticker_tracker.py

Dependencies:
    The following dependencies must be installed. See Installation section below.
        - pandas
        - requests
        - xlsxwriter

Installation:
    pip install -r requirements.txt
"""
import os
import requests
import random
from tickers import tickers
import csv
from enum import Enum
from datetime import datetime, timedelta
import pandas as pd


class ErrorCode(Enum):
    SUCCESS = 0
    ERROR_HTTP_REQUEST = 1
    ERROR_JSON_PARSING = 2
    ERROR_MISSING_FIELDS = 3
    ERROR_IO = 4
    ERROR_DIRECTORY = 5
    ERROR_UNKNOWN = 6


def scrape_and_ingest_csv(url, child_dir, t):
    """
    Grabs the historical data for a ticker and writes it as a .csv.
    """
    try:
        # Send GET request
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
        try:
            response = requests.get(url, headers=headers, timeout=5)
        except:
            print(f"Error: Failed to connect to: {url}")
            return ErrorCode.ERROR_HTTP_REQUEST

        if response.status_code != 200:
            print(f"Error: Bad status_code from: {url}")
            return ErrorCode.ERROR_HTTP_REQUEST

        # Parse response data as json
        try:
            data = response.json()
        except ValueError:
            print(f"Error: Failed to parse response as JSON for URL: {url}")
            return ErrorCode.ERROR_JSON_PARSING

        # Extract data from response
        trades_table = data.get("data", {}).get("tradesTable", {})
        if not trades_table:
            print(f"Error: 'tradesTable' not found in response data for URL: {url}")
            return ErrorCode.ERROR_MISSING_FIELDS

        headers = trades_table.get("headers", {})
        rows = trades_table.get("rows", [])
        if not headers:
            print(f"Error: No headers found in the 'tradesTable' for URL: {url}")
            return ErrorCode.ERROR_MISSING_FIELDS
        if not rows:
            print(f"Error: No rows found in the 'tradesTable' for URL: {url}")
            return ErrorCode.ERROR_MISSING_FIELDS

        # Write output to <ticker>.csv
        filepath = os.path.join(child_dir, f"{t}.csv")
        if not os.path.exists(child_dir):
            print(f"Error: The specified directory '{child_dir}' does not exist.")
            return ErrorCode.ERROR_DIRECTORY
        try:
            with open(filepath, "w", newline="") as outfile:
                writer = csv.writer(outfile)
                writer.writerow(list(headers.values()))
                for row in reversed(rows):
                    row_values = [row.get(key, "") for key in headers.keys()]
                    writer.writerow(row_values)
            return ErrorCode.SUCCESS
        except IOError as e:
            print(f"Error: Failed to write to file for URL {url}. {e}")
            return ErrorCode.ERROR_IO

    # Generic exception handler
    except Exception as e:
        print(f"Unexpected error for URL {url}: {e}")
        return ErrorCode.ERROR_UNKNOWN


def combine_csvs_to_excel(child_dir, csv_files, output_excel):
    """
    Combines multiple CSV files into a single Excel file, each CSV in its own sheet.
    """
    # Initialize a dictionary to store DataFrames
    sheets = {}

    # Iterate through the CSV files
    for f in csv_files:
        csv_file = os.path.join(child_dir, f)
        try:
            # Check if the file exists
            if not os.path.exists(csv_file):
                print(f"Error: The file {csv_file} does not exist.")
                continue

            # Try reading the CSV into a DataFrame
            df = pd.read_csv(csv_file)
            sheet_name = os.path.basename(csv_file).replace(".csv", "")
            sheets[sheet_name] = df

        except FileNotFoundError:
            print(f"File not found: {csv_file}")
        except pd.errors.EmptyDataError:
            print(f"Warning: {csv_file} is empty.")
        except pd.errors.ParserError:
            print(f"Error: Failed to parse {csv_file}.")
        except Exception as e:
            print(f"An error occurred while processing {csv_file}: {e}")

    # Check if we successfully collected data for any sheets
    if not sheets:
        print("No valid CSV files were processed. Exiting...")
        return ErrorCode.ERROR_UNKNOWN

    # Write the collected data to an Excel file
    try:
        with pd.ExcelWriter(output_excel, engine="xlsxwriter") as writer:
            for sheet_name, df in sheets.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        print(f"Success!: {output_excel}")
        return ErrorCode.SUCCESS

    except PermissionError:
        print(f"Error: Permission denied when trying to write to {output_excel}.")
    except Exception as e:
        print(f"An error occurred while writing the Excel file: {e}")


"""
Main execution block
"""
if __name__ == "__main__":
    # Ensure output/ exists
    output_directory = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), "output"
    )
    os.makedirs(output_directory, exist_ok=True)

    # Create child dir for current datetime
    now = datetime.now()
    current_datetime = now.strftime("%Y%m%d-%H%M%S")
    current_date = now.strftime("%Y-%m-%d")
    five_weeks_ago = (now - timedelta(weeks=5)).strftime("%Y-%m-%d")
    child_dir = os.path.join(output_directory, current_datetime)
    os.makedirs(child_dir, exist_ok=True)

    # Iteraate through tickers
    for t in [ticker.upper() for ticker in tickers]:
        url = f"https://api.nasdaq.com/api/quote/{t}/historical?assetclass=stocks&fromdate={five_weeks_ago}&limit=9999&todate={current_date}&random={random.randint(1, 99)}"
        rc = scrape_and_ingest_csv(url, child_dir, t)
        print(f"{t:<4} - {rc}")

    # Combine all .csvs in child_dir into one Excel file
    output_excel = os.path.join(child_dir, f"{current_date}-combined.xlsx")
    csv_files = os.listdir(child_dir)
    combine_csvs_to_excel(child_dir, csv_files, output_excel)
