# build_vectorstore.py
"""
Script to load Databricks tables, convert to Documents, embed with Ollama,
and save FAISS vectorstore to disk for sharing.
"""

import os
from typing import List, Optional
import pandas as pd
from langchain_core.documents import Document
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_community.vectorstores import FAISS
from langchain_ollama import OllamaEmbeddings
from dotenv import load_dotenv
from databricks import sql

load_dotenv("../config/.env")

# ----------------- Databricks SQL connection -----------------
def get_databricks_connection():
    return sql.connect(
        server_hostname=os.getenv("DATABRICKS_HOST"),
        http_path=os.getenv("DATABRICKS_HTTP_PATH"),
        access_token=os.getenv("DATABRICKS_TOKEN")
    )

def load_table_as_documents(
    table_name: str,
    text_cols: Optional[List[str]] = None,
    id_col: Optional[str] = None,
) -> List[Document]:
    with get_databricks_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {table_name}")
            arrow_table = cursor.fetchall_arrow()
            df = arrow_table.to_pandas()

    if text_cols is None:
        text_cols = list(df.columns)

    # Use list comprehension + itertuples
    docs = [
        Document(
            page_content="\n".join(f"{col}: {getattr(row, col)}" for col in text_cols),
            metadata={"id": getattr(row, id_col)} if id_col else {}
        )
        for row in df.itertuples(index=False)
    ]
    return docs

def build_faiss_vectorstore(
    docs: List[Document],
    chunk_size: int = 3000,
    chunk_overlap: int = 300,
    embedding_model: str = "nomic-embed-text",
    save_path: str = "vectorstore.faiss"
):
    """Chunk documents, embed them, and store in FAISS."""
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
        length_function=len,
    )
    split_docs = splitter.split_documents(docs)

    embeddings = OllamaEmbeddings(model=embedding_model)
    vectorstore = FAISS.from_documents(split_docs, embeddings, batch_size=32)

    # Save to disk
    vectorstore.save_local(save_path)
    print(f"FAISS vectorstore saved to {save_path}")

if __name__ == "__main__":
    print("Choose a table to load:")
    TABLE_NAME = input("Table name: ").strip()
    valid_tables = {
        "workspace.fars_database.accident_master",
        "workspace.fars_database.person_master",
        "workspace.fars_database.vehicle_master"
    }

    if TABLE_NAME not in valid_tables:
        print("Invalid table name. Exiting.")
        exit(1)

    print(f"Loading documents from {TABLE_NAME}...")
    docs = load_table_as_documents(TABLE_NAME)

    print("Building FAISS vectorstore...")
    build_faiss_vectorstore(docs, save_path=f"{TABLE_NAME.replace('.', '_')}_vectorstore")