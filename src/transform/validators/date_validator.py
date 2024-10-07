from src.transform.validators.validator import Validator
import pandas as pd
from typing import List, Dict, Any
import great_expectations as ge
import logging

logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

class DateValidator:
    """
    A class to validate and transform date columns in a DataFrame.
    """
    def validate_and_transform(self, data: pd.DataFrame, column: str, validation: Dict[str, Any]) -> pd.DataFrame:
        date_format = validation.get("format", "%y-%m-%d")
        try:
            # First, check if values are in date type and convert them
            data[column] = pd.to_datetime(data[column], errors='coerce')
            # Transform to the given format if necessary
            data[column] = pd.to_datetime(data[column]).dt.strftime(date_format)
            # Ensure the column remains in datetime type
            data[column] = pd.to_datetime(data[column], format=date_format, errors='coerce')
        except Exception as e:
            logging.error(f"Error while transforming column '{column}': {e}")
        return data
