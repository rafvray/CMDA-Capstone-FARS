from langchain_ollama import ChatOllama
from dotenv import load_dotenv
import os
import re 
import pandas as pd
from databricks import sql
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ------------- Load environment variables --------------
load_dotenv("../../config/.env")

# ---------------- Column Metadata ----------------
def load_column_metadata_from_sql(sql_file_path: str = "../../metadata.sql"):
    """
    Parse SQL file and extract column metadata from comments.
    Looks for patterns like: `COLUMN_NAME` type -- Description text
    """
    metadata = {
        "accident_master": {},
        "person_master": {},
        "vehicle_master": {}
    }
    
    try:
        if not os.path.exists(sql_file_path):
            return metadata
            
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Find each table definition
        current_table = None
        
        for line in content.split('\n'):
            # Detect which table we're in
            if 'CREATE TABLE `accident_master`' in line:
                current_table = "accident_master"
            elif 'CREATE TABLE `vehicle_master`' in line:
                current_table = "vehicle_master"
            elif 'CREATE TABLE `person_master`' in line:
                current_table = "person_master"
            
            # Parse column definitions with comments
            # Pattern: `COLUMN_NAME` type -- Comment text
            if current_table and '`' in line and '--' in line:
                # Extract column name between backticks
                col_match = re.search(r'`([^`]+)`', line)
                # Extract comment after --
                comment_match = re.search(r'--\s*(.+)$', line)
                
                if col_match and comment_match:
                    col_name = col_match.group(1).upper()
                    comment = comment_match.group(1).strip()
                    metadata[current_table][col_name] = comment
        
        return metadata
        
    except Exception as e:
        return metadata

COLUMN_METADATA = load_column_metadata_from_sql()

# --------- Databricks Connection and Execution ---------
def run_databricks_query(query: str) -> pd.DataFrame:
    """
    Executes a SQL query on Databricks using the official connector.
    Returns a pandas DataFrame with nullable dtypes.
    """
    try:
        with sql.connect(
            server_hostname=os.getenv("DATABRICKS_HOST"),
            http_path=os.getenv("DATABRICKS_HTTP_PATH"),
            access_token=os.getenv("DATABRICKS_TOKEN")
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                arrow_table = cursor.fetchall_arrow()
                df = arrow_table.to_pandas()
                logger.info(f"Query executed successfully, returned {len(df)} rows")
                return df
    except Exception as e:
        logger.error(f"Databricks query execution error: {str(e)}")
        raise

# ---------------- Ollama LLM ----------------
llm = None

def get_llm():
    """Lazy initialization of LLM"""
    global llm
    if llm is None:
        try:
            llm = ChatOllama(model="llama3", temperature=0)
            logger.info("LLM initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize LLM: {str(e)}")
            raise
    return llm

# ---------------- Table & Schema Info ----------------
TABLE_SCHEMAS = {
    "workspace.fars_database.accident_master": {
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
    "workspace.fars_database.person_master": {
        "columns": ["STATE", "ST_CASE", "PER_NO", "AGE", "SEX", "PER_TYP", "INJ_SEV", 
                   "SEAT_POS", "REST_USE", "REST_MIS", "HELM_USE", "HELM_MIS", "AIR_BAG", 
                   "EJECTION", "EJ_PATH", "EXTRICAT", "DRINKING", "ALC_STATUS", "ATST_TYP", 
                   "TEST_RES", "ALC_RES", "DRUGS", "DSTATUS", "HOSPITAL", "DOA", "DEATH_MO", 
                   "DEATH_DA", "DEATH_YR", "DEATH_TM", "DEATH_HR", "DEATH_MN", "LAG_HRS", 
                   "LAG_MINS", "N_MOT_NO", "STR_VEH", "DEVTYPE", "DEVMOTOR", "LOCATION", 
                   "WORK_INJ", "HISPANIC", "YEAR"],
        "string_columns": []
    },
    "workspace.fars_database.vehicle_master": {
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
                   "P_CRASH1", "P_CRASH2", "P_CRASH3", "PCRASH4", "PCRASH5", "ACC_TYPE", "ACC_CONFIG", "YEAR"],
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
        "3. NEVER apply SQL functions to columns unless their type supports it.\n"
        "4. If a question requires columns from multiple tables, ALWAYS join them using ST_CASE.\n"
        "5. ST_CASE is the primary key that exists in all three tables and is ALWAYS the join key.\n"
        "6. Prefer SUM() when a question asks for totals of numeric fields.\n"
        "7. Numeric columns must never be quoted.\n"
        "8. Use COALESCE in SUM for numeric columns: `SUM(COALESCE(column, 0))`.\n"
        "9. IMPERATIVE GROUP BY: When using ANY aggregate function (SUM, COUNT, AVG, etc.), you MUST include a GROUP BY clause with ALL non-aggregated columns from the SELECT clause.\n"
        "10. IMPERATIVE JOIN: If columns from different tables are needed, join using `ON t1.ST_CASE = t2.ST_CASE`.\n"
        "11. Output ONLY the SQL query. No comments. No markdown.\n"
        "12. NEVER reference tables that are not listed\n"
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

# ---------------- Clean LLM SQL Output ----------------
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

# ---------------------- Confirm Table Names ---------------------
FULLY_QUALIFIED_TABLES = {
    "accident_master": "workspace.fars_database.accident_master",
    "person_master": "workspace.fars_database.person_master",
    "vehicle_master": "workspace.fars_database.vehicle_master"
}

def qualify_table_names(sql_query: str) -> str:
    for short_name, fq_name in FULLY_QUALIFIED_TABLES.items():
        # only replace short_name if it is NOT already part of a fully-qualified name
        pattern = rf"(?<!\.)\b{short_name}\b" 
        sql_query = re.sub(pattern, fq_name, sql_query)
    return sql_query

def get_column_metadata_context(df: pd.DataFrame, sql_query: str) -> str:
    """
    Extract metadata for columns that appear in the query results.
    Returns a formatted string explaining what each column means.
    """
    if not COLUMN_METADATA:
        return ""
    
    context_lines = []
    columns_in_result = list(df.columns)
    
    # Determine which table(s) were queried
    query_lower = sql_query.lower()
    tables_used = []
    if 'accident_master' in query_lower:
        tables_used.append('accident_master')
    if 'vehicle_master' in query_lower:
        tables_used.append('vehicle_master')
    if 'person_master' in query_lower:
        tables_used.append('person_master')
    
    # If no tables detected, search all
    if not tables_used:
        tables_used = ['accident_master', 'vehicle_master', 'person_master']
    
    for col in columns_in_result:
        col_upper = col.upper()
        found = False
        
        # Search through the tables used in the query first
        for table_name in tables_used:
            if table_name in COLUMN_METADATA and col_upper in COLUMN_METADATA[table_name]:
                description = COLUMN_METADATA[table_name][col_upper]
                context_lines.append(f"- {col}: {description}")
                found = True
                break
        
        # If not found in used tables, search all tables
        if not found:
            for table_name, table_metadata in COLUMN_METADATA.items():
                if col_upper in table_metadata:
                    description = table_metadata[col_upper]
                    context_lines.append(f"- {col}: {description}")
                    found = True
                    break
        
        # Handle aggregated columns
        if not found and any(keyword in col_upper for keyword in ['COUNT', 'SUM', 'AVG', 'TOTAL', 'NUM']):
            context_lines.append(f"- {col}: Calculated/aggregated value")
    
    if context_lines:
        return "\n\nColumn Meanings:\n" + "\n".join(context_lines)
    return ""

# ---------------- LLM Explanation --------------------
def llm_explanation(question: str, df: pd.DataFrame, sql_query: str = "") -> str:
    """
    Uses Ollama to convert the Databricks query result into a natural language answer.
    """
    try:
        llm = get_llm()
        
        # Handle empty results
        if df.empty:
            return "The query returned no results. This might mean there's no data matching your criteria."
        
        # convert dataframe to JSON (safer + shorter)
        result_json = df.to_dict(orient="records")

        # Get metadata context for the columns in the result
        metadata_context = get_column_metadata_context(df, sql_query)

        prompt = (
            "You are an expert data interpreter.\n"
            "The user asked the following question:\n"
            f"QUESTION: {question}\n\n"
            "Here is the SQL result from Databricks:\n"
            f"{result_json}\n"
            f"{metadata_context}\n\n"
            "IMPORTANT: Use the column meanings above to interpret numeric codes correctly.\n"
            "For example, if WEATHER=1 means 'Clear', say 'Clear' not '1' in your answer.\n"
            "Write a short, clean English answer explaining the result. Do NOT mention SQL, tables, or databases."
        )

        response = llm.invoke(prompt)
        return response.content.strip()
    except Exception as e:
        logger.error(f"Error generating explanation: {str(e)}")
        return f"Query executed successfully but explanation generation failed: {str(e)}"

# ---------------- Main Query Function ----------------
def ask_fars_database(question: str, max_retries: int = 0):
    """
    Main function to process natural language questions and return SQL results.
    Returns a dict with 'query', 'results', and 'answer' keys.
    """
    try:
        llm = get_llm()
        
        tables = list(TABLE_SCHEMAS.keys())
        schema_prompt = build_schema_prompt(tables)

        # ------------------------ SQL GENERATION ------------------------
        prompt = (
            f"{schema_prompt}"
            f"Question: {question}\n"
            "Write ONLY the valid Databricks SQL query, starting with SELECT or WITH and ending with a semicolon."
        )
        
        logger.info(f"Generating SQL for question: {question}")
        response = llm.invoke(prompt)
        sql_query = clean_sql_output(response.content.strip())
        sql_query = qualify_table_names(sql_query)
        
        logger.info(f"Generated SQL: {sql_query}")

        # Check if SQL was actually generated
        if not sql_query or sql_query == ";":
            error_msg = "Failed to generate valid SQL query from the question."
            logger.error(error_msg)
            return {
                "query": None,
                "results": pd.DataFrame(),
                "answer": error_msg
            }

        # ------------------------ SQL EXECUTION ------------------------
        try:
            df = run_databricks_query(sql_query)
            nl_answer = llm_explanation(question, df, sql_query)

            return {
                "query": sql_query,
                "results": df,
                "answer": nl_answer
            }
        except Exception as e:
            error_msg = f"SQL execution error: {str(e)}"
            logger.error(error_msg)
            return {
                "query": sql_query,
                "results": pd.DataFrame(),
                "answer": error_msg
            }
            
    except Exception as e:
        error_msg = f"Error in ask_fars_database: {str(e)}"
        logger.exception(error_msg)
        return {
            "query": None,
            "results": pd.DataFrame(),
            "answer": error_msg
        }