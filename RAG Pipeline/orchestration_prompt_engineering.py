# Namrata
"""
orchestration_prompt_engineering.py

SQL-ONLY VERSION
----------------
This version disables RAG entirely.
We keep the RAG code commented out so it can be restored later.

Active components:
- ask_fars_database (SQL generator + executor)
- Router LLM (still used but always forces "sql")
"""

import os
from typing import Literal
from dotenv import load_dotenv

load_dotenv("../config/.env")

# ----------------------------------------------
# 1) IMPORT YOUR SQL MODULE
# ----------------------------------------------
from sql_query_chain import ask_fars_database
# ask_fars_database(question: str) → returns SQL result from Databricks


# ----------------------------------------------
# 2) (DISABLED) RAG MODULE
# ----------------------------------------------
"""
from faiss_rag_retriever import (
    load_table_as_documents,
    build_faiss_vectorstore,
    build_simple_rag_qa,
)

RAG_TABLES = [
    "accident_master",
    "person_master",
    "vehicle_master"
]

print("Loading documents from ALL FARS tables for RAG...")

all_docs = []
for table in RAG_TABLES:
    print(f"Loading {table}...")
    table_docs = load_table_as_documents(table)
    all_docs.extend(table_docs)

print(f"Total documents loaded: {len(all_docs)}")

print("Building unified FAISS vectorstore across all tables...")
vectorstore = build_faiss_vectorstore(all_docs)

print("Building Simple RAG QA system...")
rag_qa = build_simple_rag_qa(vectorstore)

def run_rag(question: str) -> str:
    answer, _ = rag_qa.answer(question)
    return answer
"""


# ----------------------------------------------
# 3) ROUTER LLM — NOW ALWAYS RETURNS SQL
# ----------------------------------------------
from langchain_ollama import ChatOllama

router_llm = ChatOllama(model="llama3", temperature=0)

# We keep the prompt but the router ultimately forces "sql"
ROUTER_PROMPT = """
You are a routing classifier.
Choose which system should answer the user's question.

Return ONLY one of these EXACT labels:

- "sql"  → numeric summaries, aggregates, counts, filtering, joins
- "rag"  → descriptive/explanatory questions, narrative information
- "both" → requires both a SQL numeric result + a written explanation

QUESTION:
{question}

LABEL ONLY:
"""


def route(question: str) -> Literal["sql"]:
    """SQL-only override."""
    # Even if the router predicts rag/both, force SQL mode.
    return "sql"


# ----------------------------------------------
# 4) MAIN ORCHESTRATION LOGIC (SQL ONLY)
# ----------------------------------------------
def answer_question(question: str):
    """
    Route question → SQL.
    SQL runs through ask_fars_database().
    RAG is disabled.
    """
    choice = route(question)  # always "sql"

    # --- SQL only ---
    return ask_fars_database(question)


# ----------------------------------------------
# 5) CLI Interface
# ----------------------------------------------
if __name__ == "__main__":
    print("SQL-ONLY Assistant Ready (Ollama + Databricks)")
    print("Type 'quit' to exit.\n")

    while True:
        q = input("You: ").strip()
        if q.lower() in ("quit", "exit"):
            break

        print("\nAssistant:", answer_question(q), "\n")