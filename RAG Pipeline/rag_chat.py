# rag_chat.py
"""
Interactive RAG QA using pre-built FAISS vectorstore and Ollama LLM.
"""

from langchain_community.vectorstores import FAISS
from langchain_ollama import ChatOllama
from langchain_core.documents import Document
from typing import List, Tuple

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
    vectorstore_path: str,
    llm_model: str = "llama3",
    temperature: float = 0.0,
    k: int = 4,
) -> SimpleRAGQA:
    vectorstore = FAISS.load_local(vectorstore_path)
    retriever = vectorstore.as_retriever(search_type="similarity", search_kwargs={"k": k})
    llm = ChatOllama(model=llm_model, temperature=temperature)
    return SimpleRAGQA(retriever=retriever, llm=llm)

def interactive_chat(rag_qa: SimpleRAGQA):
    print("RAG chat with Ollama ready. Type 'exit' or 'quit' to end.\n")
    while True:
        user_q = input("You: ").strip()
        if user_q.lower() in {"exit", "quit"}:
            print("Goodbye!")
            break
        if not user_q:
            continue
        answer, _ = rag_qa.answer(user_q)
        print("\nAssistant:", answer, "\n")

if __name__ == "__main__":
    VECTORSTORE_PATH = input("Enter FAISS vectorstore path: ").strip()
    rag_qa = build_simple_rag_qa(VECTORSTORE_PATH)
    interactive_chat(rag_qa)