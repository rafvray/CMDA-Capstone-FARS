# Felix

"""
RAG pipeline using:
- Ollama embeddings + FAISS as vector store
- Ollama (local, free) as the LLM
- A simple custom Retrieval-QA function

Now supports loading data directly via a Databricks SQL connection using databricks-sql-connector.
"""

import os
from typing import List, Optional, Tuple

import pandas as pd
from langchain_core.documents import Document
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_community.vectorstores import FAISS
from langchain_ollama import ChatOllama, OllamaEmbeddings

from dotenv import load_dotenv
load_dotenv("../config/.env")

# ------------------------------------------------------------------------
# Load dataset from local CSV
# ------------------------------------------------------------------------
def load_dataset_as_documents(
    csv_path: str,
    text_cols: Optional[List[str]] = None,
    id_col: Optional[str] = None,
) -> List[Document]:
    """
    Load a CSV and convert rows to LangChain Documents.
    """
    df = pd.read_csv(csv_path)

    if text_cols is None:
        text_cols = list(df.columns)

    docs: List[Document] = []
    for _, row in df.iterrows():
        parts = [f"{col}: {row[col]}" for col in text_cols]
        text = "\n".join(parts)

        metadata = {}
        if id_col and id_col in df.columns:
            metadata["id"] = row[id_col]

        docs.append(Document(page_content=text, metadata=metadata))
    return docs


# ----------------- Databricks SQL connection -----------------
from databricks import sql

def get_databricks_connection():
    return sql.connect(
        server_hostname=os.getenv("DATABRICKS_HOST"),
        http_path=os.getenv("DATABRICKS_HTTP_PATH"),
        access_token=os.getenv("DATABRICKS_TOKEN")
    )

# ------------------------------------------------------------------------
# Load table via Databricks SQL and convert to Documents
# ------------------------------------------------------------------------
def load_table_as_documents(
    table_name: str,
    text_cols: Optional[List[str]] = None,
    id_col: Optional[str] = None,
) -> List[Document]:
    """
    Load a Databricks table using SQL and convert rows to LangChain Documents.
    """
    with get_databricks_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {table_name}")
            arrow_table = cursor.fetchall_arrow()
            df = arrow_table.to_pandas()

    if text_cols is None:
        text_cols = list(df.columns)

    docs: List[Document] = []
    for _, row in df.iterrows():
        parts = [f"{col}: {row[col]}" for col in text_cols]
        text = "\n".join(parts)

        metadata = {}
        if id_col and id_col in df.columns:
            metadata["id"] = row[id_col]

        docs.append(Document(page_content=text, metadata=metadata))
    return docs


# ------------------------------------------------------------------------
# Chunk, embed, and store in FAISS
# ------------------------------------------------------------------------
def build_faiss_vectorstore(
    docs: List[Document],
    chunk_size: int = 1000,
    chunk_overlap: int = 200,
    embedding_model: str = "nomic-embed-text",
) -> FAISS:
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
        length_function=len,
    )
    split_docs = splitter.split_documents(docs)

    embeddings = OllamaEmbeddings(model=embedding_model)
    vectorstore = FAISS.from_documents(split_docs, embeddings)
    return vectorstore


# ------------------------------------------------------------------------
# Simple Retrieval-QA
# ------------------------------------------------------------------------
class SimpleRAGQA:
    def __init__(self, retriever, llm: ChatOllama):
        self.retriever = retriever
        self.llm = llm

    def answer(self, query: str) -> Tuple[str, List[Document]]:
        source_docs: List[Document] = self.retriever.get_relevant_documents(query)
        context = "\n\n".join(doc.page_content for doc in source_docs)

        prompt = f"""
        You are a helpful assistant answering questions based on the provided context.

        Use ONLY the information in the context to answer the question. If the answer
        is not in the context, say you don't know.

        Context:
        {context}

        Question:
        {query}

        Answer in clear, concise English:
        """

        response = self.llm.invoke(prompt)
        answer_text = getattr(response, "content", str(response))
        return answer_text.strip(), source_docs


def build_simple_rag_qa(
    vectorstore: FAISS,
    llm_model: str = "llama3",
    temperature: float = 0.0,
    k: int = 4,
) -> SimpleRAGQA:
    retriever = vectorstore.as_retriever(
        search_type="similarity",
        search_kwargs={"k": k},
    )

    llm = ChatOllama(
        model=llm_model,
        temperature=temperature,
    )

    return SimpleRAGQA(retriever=retriever, llm=llm)


# ------------------------------------------------------------------------
# Interactive chat
# ------------------------------------------------------------------------
def interactive_chat(rag_qa: SimpleRAGQA):
    print("RAG chat with Ollama ready. Type 'exit' or 'quit' to end.\n")

    while True:
        try:
            user_q = input("You: ").strip()
        except EOFError:
            break

        if user_q.lower() in {"exit", "quit"}:
            print("Goodbye!")
            break

        if not user_q:
            continue

        answer, _ = rag_qa.answer(user_q)
        print("\nAssistant:", answer, "\n")


# ------------------------------------------------------------------------
# Main entry
# ------------------------------------------------------------------------
if __name__ == "__main__":
    EMBEDDING_MODEL = "nomic-embed-text"
    LLM_MODEL = "llama3"

    # Let the user choose the table
    print("Choose a table to load (workspace.fars_database.accident_master, workspace.fars_database.person_master, workspace.fars_database.vehicle_master):")
    TABLE_NAME = input("Table name: ").strip()

    if TABLE_NAME not in {"workspace.fars_database.accident_master", "workspace.fars_database.person_master", "workspace.fars_database.vehicle_master"}:
        print("Invalid table name. Exiting.")
        exit(1)

    print(f"Loading documents from {TABLE_NAME}...")
    documents = load_table_as_documents(TABLE_NAME)

    print("Building FAISS vector store...")
    vectorstore = build_faiss_vectorstore(documents, embedding_model=EMBEDDING_MODEL)

    print("Building Simple RAG QA system...")
    rag_qa = build_simple_rag_qa(vectorstore, llm_model=LLM_MODEL)

    interactive_chat(rag_qa)