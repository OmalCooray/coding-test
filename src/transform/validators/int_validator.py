from src.transform.validators.validator import Validator
import pandas as pd
from typing import List, Dict, Any
import great_expectations as ge
import logging

logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

class IntValidator(Validator):
    def validate_and_transform(self, data: pd.DataFrame, column: str, validation: Dict[str, Any]) -> pd.DataFrame:
        expectation = ge.from_pandas(data).expect_column_values_to_be_of_type(column, "int")
        if not expectation.success:
            logging.error(f"Column '{column}' contains invalid integer values. Attempting to transform.")
            try:
                data[column] = pd.to_numeric(data[column], errors='coerce', downcast='integer')
            except Exception as e:
                logging.error(f"Error while transforming column '{column}': {e}")
        return data