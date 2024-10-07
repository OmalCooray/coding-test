import pymysql
import logging
import os
from typing import List

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class TransformDataInSql:
    """
    A class to execute SQL scripts from files against a MySQL database.
    """
    def __init__(self, host: str, user: str, password: str):
        """
        Initialize MySQL connection parameters.

        :param host: MySQL server host.
        :param user: MySQL username.
        :param password: MySQL password.
        """
        self.host = host
        self.user = user
        self.password = password

    def execute_scripts_from_files(self, script_files: List[str]) -> None:
        """
        Execute SQL scripts from a list of files.

        :param script_files: List of SQL script file names to be executed.
        """
        try:
            connection = pymysql.connect(
                host=self.host,
                user=self.user,
                password=self.password
            )
            with connection.cursor() as cursor:
                for file_name in script_files:
                    try:
                        with open(file_name, 'r') as file:
                            script = file.read()
                            cursor.execute(script)
                            logging.info(f"Successfully executed script from file: {file_name}")
                    except Exception as e:
                        logging.error(f"Error executing script from file: {file_name}, Error: {e}")
                connection.commit()
        except pymysql.MySQLError as e:
            logging.error(f"Error while connecting to MySQL: {e}")
        finally:
            if connection:
                connection.close()
                logging.info("MySQL connection is closed.")