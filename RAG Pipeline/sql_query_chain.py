# Rafael

from langchain_ollama import ChatOllama
from langchain_community.utilities import SQLDatabase
from dotenv import load_dotenv
import os
import ast
import re 
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

# ---------------- Result Parsers ----------------
def clean_sql_output(sql_text):
    sql_text = sql_text.strip()
    sql_text = re.sub(r"^\s*```[a-zA-Z]*\n?|\n?```\s*$", "", sql_text, flags=re.MULTILINE).strip()
    start_keywords = ["SELECT", "WITH"]
    start_idx = -1
    for kw in start_keywords:
        idx = sql_text.upper().find(kw)
        if idx != -1:
            start_idx = idx
            break
    if start_idx != -1:
        sql_text = sql_text[start_idx:]
    final_semicolon_idx = sql_text.rfind(";")
    if final_semicolon_idx != -1:
        sql_text = sql_text[:final_semicolon_idx + 1]
    return sql_text.strip()

def safe_fetch_single_value(result):
    if result is None or result in ('[]', '[()]'):
        return 0
    if isinstance(result, str):
        try:
            result = ast.literal_eval(result)
        except:
            return 0
    if isinstance(result, (list, tuple)) and len(result) > 0:
        first_row = result[0]
        if isinstance(first_row, (list, tuple)) and len(first_row) > 0:
            val = first_row[0]
            return val if val is not None else 0
        return result[0] if result[0] is not None else 0
    return result if result is not None else 0

# ---------------- Main Query Function ----------------
def ask_fars_database(question: str, max_retries: int = 2):
    tables = ["accident_master", "person_master", "vehicle_master"]
    schema_prompt = build_schema_prompt(tables)

    def generate_sql(question, error_msg=None):
        feedback = ""
        if error_msg:
            if "PARSE_SYNTAX_ERROR" in error_msg:
                feedback = "\nPrevious SQL error: PARSE_SYNTAX_ERROR. Double-check syntax.\n"
            elif "MISSING_GROUP_BY" in error_msg:
                feedback = "\nPrevious SQL error: Missing GROUP BY.\n"
            elif "CAST_INVALID_INPUT" in error_msg:
                feedback = "\nPrevious SQL error: Tried to compare numeric column with string.\n"
            else:
                feedback = f"\nPrevious SQL error: {error_msg}\nFix SQL accordingly.\n"

        prompt = (
            f"{schema_prompt}"
            f"{feedback}"
            f"Question: {question}\n"
            "Write ONLY the valid SQL query."
        )

        sql_text = llm.invoke(prompt).content
        return clean_sql_output(sql_text)

    sql_query = generate_sql(question)
    print("Generated SQL:", sql_query)

    attempts = 0
    while attempts <= max_retries:
        try:
            result = db.run(sql_query)
            if question.lower().startswith("show me"):
                return result
            return safe_fetch_single_value(result)
        except Exception as e:
            print(f"\nSQL execution error: {e}")
            attempts += 1
            if attempts > max_retries:
                return f"Failed after {max_retries} retries: {e}"
            print("\nRegenerating SQL using error feedback...")
            sql_query = generate_sql(question, str(e))
            print("Corrected SQL:", sql_query)