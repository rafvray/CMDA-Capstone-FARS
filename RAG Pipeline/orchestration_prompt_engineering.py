# Namrata

"""
orchestration_prompt_engineering.py

Unified SQL + RAG Assistant using:
- Ollama for SQL generation
- FAISS + Ollama for RAG retrieval
- Router LLM to pick SQL, RAG, or BOTH
- Databricks to run SQL queries
"""

import os
from typing import Literal
from dotenv import load_dotenv

load_dotenv("../config/.env")

# ----------------------------------------------
# 1) IMPORT SQL MODULE
# ----------------------------------------------
from langchain_community.utilities import SQLDatabase
from langchain_community.agent_toolkits.sql.base import create_sql_agent
from langchain_ollama import ChatOllama

# ---------------- Databricks Connection ----------------
db = SQLDatabase.from_databricks(
    catalog="workspace",
    schema="fars_database",
    api_token=os.getenv("DATABRICKS_TOKEN"),
    host=os.getenv("DATABRICKS_HOST"),
    warehouse_id=os.getenv("DATABRICKS_WAREHOUSE_ID"),
    include_tables=["accident_master", "person_master", "vehicle_master"]
)

# ---- Ollama LLM for generating SQL ----
sql_llm = ChatOllama(model="llama3", temperature=0)

# ---- SQL Agent: generates SQL string only ----
sql_agent = create_sql_agent(
    llm=sql_llm,
    db=db,
    agent_type="openai-tools",  # works for generating SQL
    verbose=False
)

def run_sql_query(question: str, execute: bool = True) -> str:
    """
    Generate SQL from a natural language question.
    If execute=True, runs it on Databricks and returns results.
    Otherwise, returns the SQL string only.
    """
    try:
        # Get the SQL string from Ollama agent
        result = sql_agent.invoke({"input": question})
        sql_string = result["output"]

        # Optional: remove ``` backticks if present
        sql_string = sql_string.replace("```", "").strip()

        if not execute:
            return sql_string

        # Execute the SQL on Databricks
        with db.engine.connect() as conn:
            query_result = conn.execute(sql_string)
            rows = query_result.fetchall()
            return rows if rows else "No results"

    except Exception as e:
        return f"SQL error: {str(e)}"


# ----------------------------------------------
# 2) IMPORT RAG MODULE
# ----------------------------------------------
from faiss_rag_retriever import (
    load_table_as_documents,
    build_faiss_vectorstore,
    build_simple_rag_qa,
)

TABLE_NAME = "workspace.fars_database.accident_master"
docs = load_table_as_documents(TABLE_NAME)
vectorstore = build_faiss_vectorstore(docs)
rag_qa = build_simple_rag_qa(vectorstore)

def run_rag(question: str) -> str:
    answer, _ = rag_qa.answer(question)
    return answer


# ----------------------------------------------
# 3) ROUTER LLM
# ----------------------------------------------
router_llm = ChatOllama(model="llama3", temperature=0)

ROUTER_PROMPT = """
You are a routing classifier.

Your job is to decide which module should answer a user question:

Return ONLY one of these labels:

- "sql" → structured queries, counts, joins
- "rag" → descriptive, explanatory questions
- "both" → combination

QUESTION:
{question}

LABEL ONLY:
"""

def route(question: str) -> Literal["sql", "rag", "both"]:
    resp = router_llm.invoke(ROUTER_PROMPT.format(question=question))
    label = resp.content.strip().lower()

    if "sql" in label:
        return "sql"
    if "rag" in label:
        return "rag"
    if "both" in label:
        return "both"
    return "rag"


# ----------------------------------------------
# 4) MAIN ORCHESTRATION LOGIC
# ----------------------------------------------
def answer_question(question: str, execute_sql: bool = True) -> str:
    choice = route(question)

    if choice == "sql":
        return run_sql_query(question, execute=execute_sql)

    if choice == "rag":
        return run_rag(question)

    if choice == "both":
        sql_answer = run_sql_query(question, execute=execute_sql)
        rag_answer = run_rag(question)
        return f"""
Combined Answer:

🔢 **Data Result (SQL)**  
{sql_answer}

📖 **Explanation (RAG)**  
{rag_answer}
"""


# ----------------------------------------------
# 5) CLI Interface
# ----------------------------------------------
if __name__ == "__main__":
    print("Unified SQL + RAG Assistant (Ollama + FAISS + Databricks)")
    print("Type 'quit' to exit.\n")

    while True:
        q = input("You: ").strip()
        if q.lower() in ("quit", "exit"):
            break
        print("\nAssistant:", answer_question(q), "\n")