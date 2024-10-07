import csv
import pandas as pd
import logging
import json
import os
from typing import List, Dict, Any

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class CSVExtractor:
    """
    A class to read data from a CSV file.
    """
    def __init__(self, file_path: str):
        """
        Initialize with the CSV file path.

        :param file_path: Path to the CSV file.
        """
        self._file_path = file_path

    def read_data(self) -> pd.DataFrame:
        """
        Read data from the CSV file and return as a pandas DataFrame.

        :return: DataFrame containing the CSV data.
        """
        try:
            data = pd.read_csv(self._file_path, quotechar='"', skipinitialspace=True)
            data.columns = data.columns.str.strip()  # Strip white space from column names
            for col in data.select_dtypes(include='object').columns:
                data[col] = data[col].str.strip().str.replace('"', '')  # Strip white space and remove quotation characters
            logging.info(f"{self._file_path} read successfully")
        except FileNotFoundError:
            logging.error(f"Error: File '{self._file_path}' not found.")
            data = pd.DataFrame()
        except IOError:
            logging.error(f"Error: Could not read file '{self._file_path}'.")
            data = pd.DataFrame()
        return data
    
    def read_schema(self) -> Dict[str, Any]:
        """
        Read schema from a JSON file in the schemas folder with the same name as the CSV file.

        :return: Dictionary containing the schema definition.
        """
        schema_file = os.path.join("schemas", f"{os.path.splitext(os.path.basename(self._file_path))[0]}.json")
        try:
            with open(schema_file, 'r') as f:
                schema = json.load(f)
        except FileNotFoundError:
            logging.error(f"Error: Schema file '{schema_file}' not found.")
            schema = {}
        except IOError:
            logging.error(f"Error: Could not read schema file '{schema_file}'.")
            schema = {}
        return schema