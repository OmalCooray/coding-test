from src.transform.validators.validator import Validator
import pandas as pd
from typing import List, Dict, Any
import great_expectations as ge
import logging

logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

class FloatValidator:
    """
    A class to validate and transform float columns in a DataFrame.
    """
    def validate_and_transform(self, data: pd.DataFrame, column: str, validation: Dict[str, Any]) -> pd.DataFrame:
        try:
            # Remove commas from the float values to support formats like "189,523.50"
            data[column] = data[column].str.replace(',', '')
            # Remove percentage sign and convert percentages to floats (e.g., "7.16%" -> 7.16)
            data[column] = data[column].str.replace('%', '').astype(float)
            # Convert to numeric type, coercing errors to NaN
            data[column] = pd.to_numeric(data[column], errors='coerce')
            # Validate using Great Expectations
            expectation = ge.from_pandas(data).expect_column_values_to_be_of_type(column, "float")
            if not expectation.success:
                logging.error(f"Column '{column}' contains invalid float values after transformation.")
        except Exception as e:
            logging.error(f"Error while transforming column '{column}': {e}")
        return data