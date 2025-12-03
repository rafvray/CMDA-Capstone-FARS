# full_orchestration.py
"""
Hybrid SQL + RAG Orchestration
-----------------------------
This version dynamically routes questions to:
- SQL (numeric summaries, counts, filtering, joins)
- RAG (descriptive/explanatory answers)
- Both (numeric SQL + explanatory answer)
"""

import os
from typing import List, Literal
from dotenv import load_dotenv
load_dotenv("../config/.env")

# ----------------------------------------------
# 1) IMPORT MODULES
# ----------------------------------------------
from sql_query_chain import ask_fars_database  # SQL executor
from faiss_rag_retriever import (
    load_table_as_documents,
    build_faiss_vectorstore,
    build_simple_rag_qa,
)
from langchain_ollama import ChatOllama

# ----------------------------------------------
# 2) RAG SETUP
# ----------------------------------------------
# Memory cache for vectorstores per table
VECTORSTORE_CACHE = {}
ALL_TABLES = ["workspace.fars_database.accident_master", "workspace.fars_database.person_master", "workspace.fars_database.vehicle_master"]
def run_rag(question: str, tables=ALL_TABLES) -> str:
    """
    Build RAG vectorstore dynamically for only the requested tables.
    Uses in-memory caching to avoid reloading/embedding tables repeatedly.
    """
    all_docs = []

    # Load and cache documents per table
    for table in tables:
        if table in VECTORSTORE_CACHE:
            print(f"Using cached vectorstore for {table}...")
            vectorstore = VECTORSTORE_CACHE[table]
        else:
            print(f"Loading {table} for RAG...")
            table_docs = load_table_as_documents(table)
            if not table_docs:
                continue

            print(f"Building FAISS vectorstore for {table}...")
            vectorstore = build_faiss_vectorstore(table_docs)

            # Cache the vectorstore
            VECTORSTORE_CACHE[table] = vectorstore

        all_docs.append(vectorstore)

    if not all_docs:
        return "No documents available for the requested tables."

    # Combine vectorstores into a single retriever
    if len(all_docs) == 1:
        combined_vectorstore = all_docs[0]
    else:
        # Concatenate FAISS indexes (simple approach: merge all docs)
        combined_docs = []
        for vs in all_docs:
            combined_docs.extend(vs.index_to_docstore.values())
        combined_vectorstore = build_faiss_vectorstore(combined_docs)

    # Build RAG QA
    rag_qa = build_simple_rag_qa(combined_vectorstore)
    answer, _ = rag_qa.answer(question)
    return answer

# ----------------------------------------------
# 3) ROUTER LLM
# ----------------------------------------------
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
    prompt = ROUTER_PROMPT.format(question=question)
    response = router_llm.invoke(prompt)
    label = getattr(response, "content", str(response)).strip().lower()
    if label not in {"sql", "rag", "both"}:
        label = "sql"  # fallback
    return label

# ----------------------------------------------
# 4) ORCHESTRATION
# ----------------------------------------------
def answer_question(question: str):
    choice = route(question)

    if choice == "sql":
        return ask_fars_database(question)
    elif choice == "rag":
        return run_rag(question)
    else:  # both
        sql_result = ask_fars_database(question)
        rag_result = run_rag(question)
        return f"SQL Result:\n{sql_result}\n\nExplanation:\n{rag_result}"

# ----------------------------------------------
# 5) CLI
# ----------------------------------------------
if __name__ == "__main__":
    print("Hybrid SQL + RAG Assistant Ready (Ollama + Databricks)")
    print("Type 'quit' to exit.\n")

    while True:
        q = input("You: ").strip()
        if q.lower() in ("quit", "exit"):
            break
        print("\nAssistant:", answer_question(q), "\n")
