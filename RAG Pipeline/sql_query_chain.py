# Rafael

from langchain_ollama import ChatOllama
from langchain_community.utilities import SQLDatabase
from dotenv import load_dotenv
import os

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

# ---------------- Table & Schema Helpers ----------------

# ask fars_database with table selection + schema prompt + SQL execution
def get_relevant_tables(question: str):
    q = question.lower()
    tables = set()

    accident_keywords = ["fatal", "fatalities", "weather", "accident", "year", "state", "crash"]
    person_keywords   = ["age", "driver", "person", "sex", "injur", "victim"]
    vehicle_keywords  = ["vehicle", "make", "model", "car", "truck", "motorcycle"]

    if any(word in q for word in accident_keywords):
        tables.add("accident_master")
    if any(word in q for word in person_keywords):
        tables.add("person_master")
    if any(word in q for word in vehicle_keywords):
        tables.add("vehicle_master")

    # ---------- FALLBACK ----------
    if not tables:
        # No match → include all tables so SQL can still succeed
        return ["accident_master", "person_master", "vehicle_master"]

    return list(tables)

TABLE_SCHEMAS = {
    "accident_master": ["STATE", "ST_CASE", "PEDS", "PERNOTMVIT", "VE_TOTAL", "VE_FORMS", 
                        "PVH_INVL", "PERSONS", "PERMVIT", "COUNTY", "CITY", "MONTH", "DAY", 
                        "DAY_WEEK", "YEAR", "HOUR", "MINUTE", "TWAY_ID", "TWAY_ID2", "CL_TWAY", 
                        "ROUTE", "RUR_URB", "FUNC_SYS","RD_OWNER", "NHS", "SP_JUR", "MILEPT", 
                        "LATITUDE", "LONGITUD", "HARM_EV", "MAN_COLL", "RELJCT1", "REL_JUNC", 
                        "RELJCT2", "TYP_INT", "REL_ROAD", "C_M_ZONE", "WRK_ZONE", "LGT_COND", 
                        "WEATHER", "SCH_BUS", "RAIL", "NOT_HOUR", "NOT_MIN", "ARR_HOUR", 
                        "ARR_MIN", "HOSP_HR", "HOSP_MN", "FATALS"],

    "person_master": ["STATE", "ST_CASE", "PER_NO", "AGE", "SEX", "PER_TYP", "INJ_SEV", 
                      "SEAT_POS", "REST_USE", "REST_MIS", "HELM_USE", "HELM_MIS", "AIR_BAG", 
                      "EJECTION", "EJ_PATH", "EXTRICAT", "DRINKING", "ALC_STATUS", "ATST_TYP", 
                      "TEST_RES", "ALC_RES", "DRUGS", "DSTATUS", "HOSPITAL", "DOA", "DEATH_MO", 
                      "DEATH_DA", "DEATH_YR", "DEATH_TM", "DEATH_HR", "DEATH_MN", "LAG_HRS", 
                      "LAG_MINS", "N_MOT_NO", "STR_VEH", "DEVTYPE", "DEVMOTOR", "LOCATION", 
                      "WORK_INJ", "HISPANIC"],

    "vehicle_master": ["STATE", "ST_CASE", "VEH_NO", "OCUPANTS", "NUMOCCS", "UNITTYPE", "HIT_RUN", 
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
                       "P_CRASH1", "P_CRASH2", "P_CRASH3", "PCRASH4", "PCRASH5", "ACC_TYPE", "ACC_CONFIG"]
}

def build_schema_prompt(tables):
    prompt = (
        "You are an expert SQL generator for a Databricks SQL database.\n"
        "You MUST follow all the rules below:\n\n"
        "Rules:\n"
        "1. ONLY use the tables and columns listed.\n"
        "2. NEVER guess or invent column names.\n"
        "3. NEVER apply SQL functions to columns unless their type supports it "
        "(e.g., do not apply YEAR() to numeric columns).\n"
        "4. If a question requires columns from multiple tables, ALWAYS join "
        "them using ST_CASE.\n"
        "5. ST_CASE is the primary key that exists in all three tables and "
        "is ALWAYS the join key.\n"
        "6. Prefer SUM() when a question asks for totals of numeric fields.\n"
        "7. Output ONLY the SQL query. No comments. No markdown.\n\n"
        "Available Tables:\n"
    )

    for t in tables:
        prompt += f"- {t} columns: {', '.join(TABLE_SCHEMAS[t])}\n"

    prompt += (
        "\nWhen generating SQL, follow this reasoning structure internally "
        "(but do NOT output these steps):\n"
        "Step 1: Identify key columns needed to answer the question.\n"
        "Step 2: Identify which tables contain those columns.\n"
        "Step 3: JOIN tables using ST_CASE when needed.\n"
        "Step 4: Write valid SQL.\n"
    )

    return prompt

def ask_fars_database(question: str):
    tables = get_relevant_tables(question)
    schema_prompt = build_schema_prompt(tables)
    full_prompt = full_prompt = (
                    f"{schema_prompt}\n"
                    f"Question: {question}\n"
                    "Write ONLY the SQL query as plain text."
                )


    sql_query = llm.invoke(full_prompt).content.strip()
    sql_query = sql_query.replace("```", "").strip()
    print("Generated SQL:", sql_query)

    try:
        result = db.run(sql_query)
        if isinstance(result, list) and len(result) == 1 and len(result[0]) == 1:
            return result[0][0]
        return result
    except Exception as e:
        return f"SQL execution error: {str(e)}"