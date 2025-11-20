# Rafael

from langchain_ollama import ChatOllama
from langchain_community.utilities import SQLDatabase
from dotenv import load_dotenv
import os
import ast
import re 
import decimal
import pprint
load_dotenv("../config/.env")

# ---------------- Databricks Connection ----------------
db = SQLDatabase.from_databricks(
    catalog="workspace",
    schema="fars_database",
    api_token=os.getenv("DATABRICKS_TOKEN"),
    host=os.getenv("DATABRICKS_HOST"),
    warehouse_id=os.getenv("DATABRICKS_WAREHOUSE_ID"),
    include_tables=["accident_master", "person_master", "vehicle_master"]
)

# ---------------- Ollama LLM ----------------
llm = ChatOllama(model="llama3", temperature=0)

# ---------------- Table & Schema Info ----------------
TABLE_SCHEMAS = {
    "accident_master": {
        "columns": ["STATE", "ST_CASE", "PEDS", "PERNOTMVIT", "VE_TOTAL", "VE_FORMS", 
                    "PVH_INVL", "PERSONS", "PERMVIT", "COUNTY", "CITY", "MONTH", "DAY", 
                    "DAY_WEEK", "YEAR", "HOUR", "MINUTE", "TWAY_ID", "TWAY_ID2", "CL_TWAY", 
                    "ROUTE", "RUR_URB", "FUNC_SYS","RD_OWNER", "NHS", "SP_JUR", "MILEPT", 
                    "LATITUDE", "LONGITUD", "HARM_EV", "MAN_COLL", "RELJCT1", "REL_JUNC", 
                    "RELJCT2", "TYP_INT", "REL_ROAD", "C_M_ZONE", "WRK_ZONE", "LGT_COND", 
                    "WEATHER", "SCH_BUS", "RAIL", "NOT_HOUR", "NOT_MIN", "ARR_HOUR", 
                    "ARR_MIN", "HOSP_HR", "HOSP_MN", "FATALS"],
        "string_columns": ["RAIL", "TWAY_ID", "TWAY_ID2"]
    },
    "person_master": {
        "columns": ["STATE", "ST_CASE", "PER_NO", "AGE", "SEX", "PER_TYP", "INJ_SEV", 
                   "SEAT_POS", "REST_USE", "REST_MIS", "HELM_USE", "HELM_MIS", "AIR_BAG", 
                   "EJECTION", "EJ_PATH", "EXTRICAT", "DRINKING", "ALC_STATUS", "ATST_TYP", 
                   "TEST_RES", "ALC_RES", "DRUGS", "DSTATUS", "HOSPITAL", "DOA", "DEATH_MO", 
                   "DEATH_DA", "DEATH_YR", "DEATH_TM", "DEATH_HR", "DEATH_MN", "LAG_HRS", 
                   "LAG_MINS", "N_MOT_NO", "STR_VEH", "DEVTYPE", "DEVMOTOR", "LOCATION", 
                   "WORK_INJ", "HISPANIC"],
        "string_columns": []
    },
    "vehicle_master": {
        "columns": ["STATE", "ST_CASE", "VEH_NO", "OCUPANTS", "NUMOCCS", "UNITTYPE", "HIT_RUN", 
                   "REG_STAT", "OWNER", "VIN", "MOD_YEAR", "VPICMAKE", "VPICMODEL", 
                   "VPICBODYCLASS", "MAKE", "MODEL", "BODY_TYP", "ICFINALBODY", "GVWR_FROM", 
                   "GVWR_TO", "TOW_VEH", "TRLR1VIN", "TRLR2VIN", "TRLR3VIN", "TRLR1GVWR", 
                   "TRLR2GVWR", "TRLR3GVWR", "J_KNIFE", "MCARR_ID", "MCARR_I1", "MCARR_I2", 
                   "V_CONFIG", "CARGO_BT", "HAZ_INV", "HAZ_PLAC", "HAZ_ID", "HAZ_CNO", 
                   "HAZ_REL", "BUS_USE", "SPEC_USE", "EMER_USE", "TRAV_SP", "UNDEROVERRIDE", 
                   "ROLLOVER", "ROLINLOC", "IMPACT1", "DEFORMED", "TOWAWAY", "TOWED", "M_HARM", 
                   "FIRE_EXP", "ADS_PRES", "ADS_LEV", "ADS_ENG", "MAK_MOD", "VIN_1", "VIN_2", 
                   "VIN_3", "VIN_4", "VIN_5", "VIN_6", "VIN_7", "VIN_8", "VIN_9", "VIN_10",
                   "VIN_11", "VIN_12", "DEATHS", "DR_DRINK", "DR_PRES", "L_STATE", "DR_ZIP", 
                   "L_TYPE", "L_STATUS", "CDL_STAT", "L_ENDORS", "L_CL_VEH", "L_COMPL",
                   "L_RESTRI", "DR_HGT", "DR_WGT", "PREV_ACC", "PREV_SUS1", "PREV_SUS2",
                   "PREV_SUS3", "PREV_DWI", "PREV_SPD", "PREV_OTH", "FIRST_MO", "FIRST_YR",
                   "LAST_MO", "LAST_YR", "SPEEDREL", "VTRAFWAY", "VNUM_LAN", "VSPD_LIM",
                   "VALIGN", "VPROFILE", "VPAVETYP", "VSURCOND", "VTRAFCON", "VTCONT_F",
                   "P_CRASH1", "P_CRASH2", "P_CRASH3", "PCRASH4", "PCRASH5", "ACC_TYPE", "ACC_CONFIG"],
        "string_columns": ["VIN", "TRLR1VIN", "TRLR2VIN", "TRLR3VIN", "MCARR_ID", "MCARR_I2",
                           "ADS_PRES", "ADS_LEV", "ADS_ENG"] + [f"VIN_{i}" for i in range(1,13)]
    }
}

# ---------------- Schema Prompt Builder ----------------
def build_schema_prompt(tables):
    prompt = (
        "You are an expert SQL generator for a Databricks SQL database.\n"
        "You MUST follow all the rules below:\n\n"
        "Rules:\n"
        "1. ONLY use the tables and columns listed.\n"
        "2. NEVER guess or invent column names.\n"
        "3. NEVER apply SQL functions to columns unless their type supports it "
        "(e.g., do not apply YEAR() to numeric columns).\n"
        "4. If a question requires columns from multiple tables, ALWAYS join them using ST_CASE.\n"
        "5. ST_CASE is the primary key that exists in all three tables and is ALWAYS the join key.\n"
        "6. Prefer SUM() when a question asks for totals of numeric fields.\n"
        "7. Numeric columns must never be quoted.\n"
        "8. Use COALESCE in SUM for numeric columns: `SUM(COALESCE(column, 0))`.\n"
        "9. IMPERATIVE GROUP BY: Use GROUP BY for ANY column in the SELECT clause that is NOT aggregated (SUM, COUNT, etc.).\n"
        "10. IMPERATIVE JOIN: If columns from different tables are needed, join using `ON t1.ST_CASE = t2.ST_CASE`.\n"
        "11. Output ONLY the SQL query. No comments. No markdown.\n\n"
        "12. NEVER reference tables that are not listed (e.g., accident_data is invalid).\n"
        "13. **NEVER use string literals like 'DRIVER' or 'RAIN' in WHERE clauses for these columns.** Use only numeric codes (e.g., `PER_TYP = 1`).\n"
        "Available Tables and Columns:\n"
    )
    
    for t in tables:
        table_info = TABLE_SCHEMAS[t]
        prompt += f"- **{t}** (Join Key: ST_CASE):\n"
        prompt += f"  All Columns: {', '.join(table_info['columns'])}\n"
        numeric_cols = [c for c in table_info['columns'] if c not in table_info.get('string_columns', [])]
        if numeric_cols:
            prompt += f"  Numeric Columns (Use for math/comparison): {', '.join(numeric_cols)}\n"
        if table_info.get("string_columns"):
            prompt += f"  String Columns (May need quotes): {', '.join(table_info['string_columns'])}\n"
    
    prompt += (
        "\nFinal Instruction: Write ONLY the valid Databricks SQL query, starting with SELECT or WITH, and ending with a semicolon, with NO markdown formatting.\n"
    )
    
    return prompt

# ---------------- Load metadata SQL file and append to prompt ----------------
METADATA_SQL_PATH = "../metadata.sql"  # change if needed

def load_metadata_sql(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception:
        return None

METADATA_SQL_TEXT = load_metadata_sql(METADATA_SQL_PATH)
if METADATA_SQL_TEXT:
    # MAKE IT SMALL — we will append a brief excerpt to the prompt (not massive files)
    METADATA_SQL_TEXT = METADATA_SQL_TEXT.strip()
    if len(METADATA_SQL_TEXT) > 4000:
        METADATA_SQL_TEXT = METADATA_SQL_TEXT[:4000] + "\n-- (truncated) --"


# ---------------- Result Parsers ----------------
def clean_sql_output(sql_text):
    if sql_text is None:
        return ""

    # remove markdown fences
    sql_text = re.sub(r"```(?:sql)?\n?", "", sql_text, flags=re.IGNORECASE)
    sql_text = re.sub(r"\n?```$", "", sql_text, flags=re.IGNORECASE).strip()

    # try to find SELECT or WITH to the first semicolon
    match = re.search(r"((SELECT|WITH)[\s\S]*?;)", sql_text, re.IGNORECASE)
    if match:
        return match.group(1).strip()

    # fallback: ensure trailing semicolon
    sql = sql_text.strip()
    if not sql.endswith(";"):
        sql += ";"
    return sql

# In sql_query_chain.py, replace the existing safe_fetch_single_value with this:
def safe_fetch_single_value(result):
    """Safely extract a single value (e.g., count or sum) from query result."""
    
    # 1. Handle direct None or empty string result
    if result is None or (isinstance(result, str) and not result.strip()):
        return 0
    
    # 2. Attempt to evaluate string results (common LangChain behavior)
    if isinstance(result, str):
        result = result.strip()
        # Handle string representations of empty results
        if result in ('[]', '[()]', 'None'):
            return 0
        try:
            # Use ast.literal_eval to convert string representation of list/tuple/int/float
            result = ast.literal_eval(result)
        except Exception:
            # If literal_eval fails, it might be a raw numeric string.
            try:
                return int(float(result)) # Attempt to cast to number
            except:
                return 0 # Fallback for unparseable raw string

    # 3. Process list/tuple results (which should contain the final value)
    if isinstance(result, (list, tuple)) and len(result) > 0:
        first_row = result[0]
        
        # If the result is a list of rows (e.g., [(100,)]), extract the first value
        if isinstance(first_row, (list, tuple)) and len(first_row) > 0:
            val = first_row[0]
            return val if val is not None else 0
        
        # If the result is a flat list/tuple (e.g., [100]), return the first element.
        return first_row if first_row is not None else 0
    
    # 4. Handle direct numeric result
    return result if result is not None else 0

# ---------------- Main Query Function ----------------
# removed error feedback logic - implement back if needed.
def ask_fars_database(question: str, max_retries: int = 0):
    tables = ["accident_master", "person_master", "vehicle_master"]
    schema_prompt = build_schema_prompt(tables)

    if METADATA_SQL_TEXT:
        schema_prompt = schema_prompt + "\n\n-- METADATA SQL (reference):\n" + METADATA_SQL_TEXT + "\n\n"

    # ------------------------
    # SQL GENERATION (simple)
    # ------------------------
    def generate_sql(question):
        prompt = (
            f"{schema_prompt}"
            f"Question: {question}\n"
            "Write ONLY the valid Databricks SQL query, starting with SELECT or WITH and ending with a semicolon."
        )
        response = llm.invoke(prompt)
        sql_text = clean_sql_output(response.content.strip())
        return sql_text

    # --- Generate SQL (no feedback loops)
    sql_query = generate_sql(question)
    print("Generated SQL:", sql_query)

    # ------------------------
    # EXECUTE ONCE — NO RETRIES
    # ------------------------
    try:
        result = db.run(sql_query)

        print("RAW DB RESULT:", result)
        print("RAW TYPE:", type(result))

        # If "show me ..." return table
        if question.lower().startswith("show me"):
            return result

        return safe_fetch_single_value(result)

    except Exception as e:
        # Just return the error, no regeneration
        return f"SQL execution error: {str(e)}"