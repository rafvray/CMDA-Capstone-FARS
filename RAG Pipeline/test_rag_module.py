# test_rag_retriever_local.py

from faiss_rag_retriever import (
    load_dataset_as_documents,
    build_faiss_vectorstore,
    build_simple_rag_qa,
)

# Path to a local CSV copy of your accident_master table
CSV_PATH = "../../FARS Merge/master_accident.csv"  # replace with your local CSV path


def main():
    print("Building RAG retriever (local CSV)...")

    # Load raw docs from CSV
    documents = load_dataset_as_documents(
        csv_path=CSV_PATH,
        text_cols=None,  # or specify e.g. ["ST_CASE", "STATE", "FATALS", "WEATHER"]
        id_col=None,
    )

    # Build FAISS index
    vectorstore = build_faiss_vectorstore(documents)

    # Build RAG QA system
    rag_qa = build_simple_rag_qa(vectorstore)

    print("RAG ready. Ask anything about the accident_master table.\n")
    print("Type 'exit' to quit.\n")

    while True:
        q = input("You: ").strip()
        if q.lower() in {"exit", "quit"}:
            break

        answer, sources = rag_qa.answer(q)

        print("\nAnswer:", answer)
        print("\nSources returned:", len(sources))
        print("-" * 40)


if __name__ == "__main__":
    main()