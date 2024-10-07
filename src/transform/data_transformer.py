from typing import List, Dict, Any
from src.transform.validators.date_validator import DateValidator
from src.transform.validators.float_validator import FloatValidator
from src.transform.validators.int_validator import IntValidator
from src.transform.validators.str_validator import StrValidator
import pandas as pd


class DataTransformer:
    """
    A class to perform data quality checks using Great Expectations and transform data if necessary.
    """
    def __init__(self, data: pd.DataFrame, schema: Dict[str, Any]):
        """
        Initialize with the data as a pandas DataFrame and the schema definition.

        :param data: DataFrame containing the data to be validated and transformed.
        :param schema: Dictionary defining the expected data types for each column or possible value options.
        """
        self._data = data
        self._schema = schema
        self._validators = {
            "date": DateValidator(),
            "float": FloatValidator(),
            "int": IntValidator(),
            "str": StrValidator(),
        }

    def validate_and_transform(self) -> pd.DataFrame:
        """
        Validate the DataFrame against the schema and transform data if necessary.

        :return: Transformed DataFrame with corrected data types and formats.
        """
        for column, validation in self._schema.items():
            if column in self._data.columns and "dtype" in validation:
                dtype = validation["dtype"]
                validator = self._validators.get(dtype)
                if validator:
                    self._data = validator.validate_and_transform(self._data, column, validation)
        return self._data