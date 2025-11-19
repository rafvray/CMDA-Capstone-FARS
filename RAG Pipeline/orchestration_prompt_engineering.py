# Namrata
"""
orchestration_prompt_engineering.py

Unified SQL + RAG Assistant using:
- Your custom SQL generator (ask_fars_database)
- FAISS + Ollama for RAG retrieval
- Router LLM to pick SQL, RAG, or BOTH
- Databricks for SQL execution
"""

import os
from typing import Literal
from dotenv import load_dotenv

load_dotenv("../config/.env")

# ----------------------------------------------
# 1) IMPORT YOUR SQL MODULE
# ----------------------------------------------
from sql_query_chain import ask_fars_database

# ask_fars_database(question: str)
# → returns SQL result directly from Databricks
# → uses schema prompts + ST_CASE rules


# ----------------------------------------------
# 2) IMPORT RAG MODULE (Supports All Tables)
# ----------------------------------------------
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


# ----------------------------------------------
# 3) ROUTER LLM
# ----------------------------------------------
from langchain_ollama import ChatOllama

router_llm = ChatOllama(model="llama3", temperature=0)

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
def answer_question(question: str) -> str:
    """
    Route question → SQL, RAG, or both.
    SQL is executed using your ask_fars_database() helper.
    """
    choice = route(question)

    # --- SQL only ---
    if choice == "sql":
        return ask_fars_database(question)

    # --- RAG only ---
    if choice == "rag":
        return run_rag(question)

    # --- Both ---
    if choice == "both":
        sql_result = ask_fars_database(question)
        
        # New step: Ask the RAG system to interpret the SQL result
        interpretation_question = f"The following SQL query for the question '{question}' returned this result: {sql_result}. Provide an explanation or context for this finding."
        rag_text = run_rag(interpretation_question)

        return f"""
    Combined Answer:

    🔢 **SQL Data**
    The precise numeric result is: {sql_result}

    📖 **Explanation (RAG)**
    {rag_text}
    """

# ----------------------------------------------
# 5) CLI Interface
# ----------------------------------------------
if __name__ == "__main__":
    print("Unified SQL + RAG Assistant Ready (Ollama + FAISS + Databricks)")
    print("Type 'quit' to exit.\n")

    while True:
        q = input("You: ").strip()
        if q.lower() in ("quit", "exit"):
            break

        print("\nAssistant:", answer_question(q), "\n")