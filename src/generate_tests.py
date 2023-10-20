import logging
from pathlib import Path
from typing import Any, Dict

import duckdb
import pandas as pd
import sqlglot
import yaml


class DataUnitTest:
    def __init__(self, path: str, model_name: str, model_sql: str):
        self.path = path
        self.model_name = model_name
        self.model_sql = model_sql
        self.df = self.fetch_test_dataset(self.path)

    def fetch_test_dataset(self, path) -> pd.DataFrame:
        try:
            return pd.read_csv(path)
        except Exception as e:
            raise Exception(f"Error fetching test dataset: {e}")

    def extract_ctes_from_sql(self):
        parsed = sqlglot.parse(self.model_sql)
        statements = {}
        for stmt in parsed:
            try:
                if stmt.ctes:
                    cte_map = {}
                    statements["CTE"] = str(stmt)
                    for idx, cte in enumerate(stmt.ctes):
                        cte_str = str(cte)
                        name = cte_str.split("AS")[0].strip()
                        query = cte_str.split("AS", 1)[1].strip()
                        query = sqlglot.transpile(
                            query, write="duckdb", identify=True, pretty=True
                        )[0]
                        cte_map[cte.alias] = query
            except AttributeError as e:
                logging.error(f"Error extracting CTEs: {e}")

            if type(stmt).__name__ == "Command":
                stmt_type = type(stmt).expression
                statements[str(stmt_type)] = stmt
            else:
                stmt_type = type(stmt).__name__
                statements[str(stmt_type)] = stmt
        return cte_map, statements

    def fetch_cte_data(self, cte_map: Dict[str, str]) -> Dict[str, Any]:
        sql_df = self.df.copy()
        cte_data = {}
        for cte_name, cte_query in cte_map.items():
            try:
                cte_query = cte_query.removesuffix(")").removeprefix("(").strip()
                cte_query = cte_query.replace("loans", "sql_df")
                cte_df = duckdb.query(cte_query).fetch_arrow_table().to_pandas()
                cte_data[cte_name] = cte_df.to_dict("records")
            except Exception as e:
                logging.error(f"Error fetching CTE data for {cte_name}: {e}")
                raise
        return cte_data

    def generate_test_yaml(self) -> str:
        ctes, statements = self.extract_ctes_from_sql()
        test_structure = {
            TEST_ID: {
                "model": self.model_name,
                "inputs": {INPUT_MODEL: {"rows": self.df.to_dict("records")}},
                "outputs": {"query": {}, "ctes": {}},
            }
        }
        cte_data = self.fetch_cte_data(ctes)
        for cte_name, data in cte_data.items():
            test_structure[TEST_ID]["outputs"]["ctes"][cte_name] = {"rows": data}

        try:
            loans = self.df.copy()
            main_query_output = (
                duckdb.query(str(statements["CTE"])).fetch_arrow_table().to_pandas()
            )
            test_structure[TEST_ID]["outputs"]["query"][
                "rows"
            ] = main_query_output.to_dict("records")
        except Exception as e:
            logging.error(f"Error in main query: {e}")
            raise

        return yaml.dump(test_structure)

    def run(self) -> str:
        return self.generate_test_yaml()


def modify_yaml(file_path, data):
    data = yaml.load(data, Loader=yaml.FullLoader)
    model_value = data.get("model", None)
    if TEST_ID in data and "inputs" in data[TEST_ID]:
        data[TEST_ID] = {"model": model_value, **data[TEST_ID]}
    with open(file_path, "w") as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    root = Path().cwd().parent
    METRIC_DS_NM = "metric_loans"
    DATA_FILE = root / f"data/test_{METRIC_DS_NM}.csv"
    OUTPUT_FILE = root / f"tests/test_{METRIC_DS_NM}.yaml"
    SQL_FILE = root / f"sql/test_{METRIC_DS_NM}.sql"
    with open(SQL_FILE, "r") as file:
        MODEL_SQL = file.read()

    TEST_ID = f"sqlmesh_test_suite_id"
    INPUT_MODEL = f"sqlmesh_test_id.{METRIC_DS_NM}_input_model"
    MODEL_NM = f"sqlmesh_test_id.{METRIC_DS_NM}_full_model"

    tester = DataUnitTest(DATA_FILE, MODEL_NM, MODEL_SQL)
    yaml_output = tester.run()
    modify_yaml(OUTPUT_FILE, yaml_output)

    logging.info(f"Generated test file: {OUTPUT_FILE}")
