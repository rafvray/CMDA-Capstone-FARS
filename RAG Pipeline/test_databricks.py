from langchain_community.utilities import SQLDatabase
from dotenv import load_dotenv
import os

load_dotenv("../config/.env")

db = SQLDatabase.from_databricks(
    catalog="workspace",
    schema="fars_database",
    api_token=os.getenv("DATABRICKS_TOKEN"),
    host=os.getenv("DATABRICKS_HOST"),
    warehouse_id=os.getenv("DATABRICKS_WAREHOUSE_ID"),
    include_tables=["accident_master", "person_master", "vehicle_master"]
)

print(db.table_info)
