from src.transform.validators.validator import Validator
import pandas as pd
from typing import List, Dict, Any
import great_expectations as ge
import logging

logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

class StrValidator(Validator):
    def validate_and_transform(self, data: pd.DataFrame, column: str, validation: Dict[str, Any]) -> pd.DataFrame:
        expectation = ge.from_pandas(data).expect_column_values_to_be_of_type(column, "str")
        if not expectation.success:
            logging.error(f"Column '{column}' contains invalid string values. Attempting to transform.")
            try:
                data[column] = data[column].astype(str)
            except Exception as e:
                logging.error(f"Error while transforming column '{column}': {e}")
        if "categories" in validation:
            categories = validation["categories"]
            expectation = ge.from_pandas(data).expect_column_values_to_be_in_set(column, categories)
            if not expectation.success:
                logging.error(f"Column '{column}' contains values outside the allowed categories: {categories}. Attempting to transform.")
                try:
                    data[column] = data[column].apply(lambda x: x if x in categories else None)
                except Exception as e:
                    logging.error(f"Error while transforming column '{column}': {e}")
        return data