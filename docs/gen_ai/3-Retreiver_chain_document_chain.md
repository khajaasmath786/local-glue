# 📌 Retrievers, Chains, and Document Chains in LLMs

## 🔹 What is a Retriever?
- A **Retriever** is responsible for **fetching relevant documents** from a knowledge source (vector DB, keyword index, etc.).
- It takes your query, performs a **similarity search (or other retrieval method)**, and returns the top relevant chunks.
- Example: In LangChain, `.as_retriever()` wraps a database so you can query it uniformly.

👉 Think of it as the **"search engine"** part of a RAG pipeline.

---

## 🔹 What is a Chain?
- A **Chain** is a **sequence of steps** where outputs from one component become inputs to the next.
- Chains can combine:
  1. **Retriever** → fetch context
  2. **LLM** → use context + query to generate final answer
- They can also have multiple stages like validation, reasoning, or tool use.

👉 Think of it as the **"workflow"** that connects retrievers, LLMs, and other tools.

---

## 🔹 What is a Document Chain?
- A **Document Chain** is a **special type of chain** focused on processing documents.
- It takes retrieved documents and decides **how to feed them into the LLM**:
  - **Stuff** → all docs are concatenated into the LLM context.
  - **Map-Reduce** → each doc processed separately, then results combined.
  - **Refine** → LLM iteratively refines the answer with each doc.
- It’s essentially a **RAG chain that specializes in document handling**.

👉 All document chains are chains, but not all chains are document chains.

---

## 🔹 Example: Retriever + Document Chain (LangChain with FAISS)

### 1. Setup Vector Store
```python
from langchain.vectorstores import FAISS
from langchain.embeddings import HuggingFaceEmbeddings

# Example docs
docs = ["Hudi supports upserts", "Hudi also supports deletes", "Iceberg is another table format"]

# Embed and store
embeddings = HuggingFaceEmbeddings(model_name="sentence-transformers/all-MiniLM-L6-v2")
vectorstore = FAISS.from_texts(docs, embedding=embeddings)
