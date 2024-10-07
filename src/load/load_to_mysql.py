import mysql.connector
from mysql.connector import Error
import pandas as pd
import logging
from typing import Dict, Any

logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

class LoadToMySQL:
    """
    A class to load a pandas DataFrame into a MySQL database.
    """
    def __init__(self, host: str, user: str, password: str, database: str):
        """
        Initialize MySQL connection parameters.

        :param host: MySQL server host.
        :param user: MySQL username.
        :param password: MySQL password.
        :param database: MySQL database name.
        """
        self.host = host
        self.user = user
        self.password = password
        self.database = database

    def load_data(self, data: pd.DataFrame, table_name: str, schema: Dict[str, Any]) -> None:
        """
        Load the DataFrame into the MySQL database table.

        :param data: DataFrame containing the data to be loaded.
        :param table_name: Name of the MySQL table where data will be loaded.
        :param schema: Dictionary containing the schema definition for validation.
        """
        try:
            connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            if connection.is_connected():
                cursor = connection.cursor()
                columns = ', '.join([f"`{col}`" for col in data.columns])
                placeholders = ', '.join(['%s' for _ in data.columns])
                insert_query = f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})"
                for _, row in data.iterrows():
                    cursor.execute(insert_query, tuple(row))
                connection.commit()
                logging.info(f"Data loaded successfully into '{table_name}' table.")
        except Error as e:
            logging.error(f"Error while connecting to MySQL: {e}")
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()
                logging.info("MySQL connection is closed.")