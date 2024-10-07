from src.extract.csv_extractor import CSVExtractor
from src.transform.data_transformer import DataTransformer
from src.load.load_to_mysql import LoadToMySQL
from src.transform.sql_data_transformer import TransformDataInSql


if __name__ == "__main__":
    loader = LoadToMySQL(host="localhost", user="root", password="qKWRNg3OjI", database="staging")
    files_to_tables = [
        {"file_name": "data/holdings_data_1.csv", "table_name": "holdings_data"},
        {"file_name": "data/holdings_data_1.csv", "table_name": "holdings_data"},
        {"file_name": "data/portfolio_stats_1.csv", "table_name": "portfolio_stats"},
        {"file_name": "data/portfolio_stats_2.csv", "table_name": "portfolio_stats"}
    ]

    for entry in files_to_tables:
        csv_reader = CSVExtractor(entry["file_name"])
        data = csv_reader.read_data()
        if not data.empty:
            schema = csv_reader.read_schema()
            if schema:
                dq_checker = DataTransformer(data, schema)
                validated_data = dq_checker.validate_and_transform()
                loader.load_data(validated_data, entry["table_name"], schema)
                print(f"Loaded data from {entry['file_name']} into table {entry['table_name']}")

    script_files = ["sql/portfolio_stats_staging_to_portfolio_stats_fact.sql","sql/holdings_staging_to_holdings_fact.sql","sql/combined_holdings_portfolio.sql"]
    transformer = TransformDataInSql(host="localhost", user="root", password="qKWRNg3OjI")
    transformer.execute_scripts_from_files(script_files)