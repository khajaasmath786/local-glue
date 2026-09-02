# Retrieval Augmented Generation (RAG)

RAG stands for:
- **R**: Retrieval (using embeddings to search and find relevant information)
- **A**: Augmentation (adding retrieved context to your query)
- **G**: Generation (using a Large Language Model to generate the final answer)

Retrieval Augmented Generation (RAG) is a technique that enhances Large Language Models (LLMs) by allowing them to access and use external knowledge sources in real time. Instead of relying solely on what the LLM was trained on, RAG enables the model to retrieve relevant information from documents, databases, or other resources, and use that context to generate more accurate, up-to-date, and factual answers. This approach combines the strengths of LLMs (general reasoning and language understanding) with the precision and freshness of external data, making responses smarter and more reliable.



---

## Architecture

![RAG Architecture](images/rag_architecture.png)

---

## Table of Contents
- [1. Overview](#1-overview)
- [2. Architecture Diagram](#2-architecture-diagram)
- [3. RAG Modes](#3-rag-modes)
  - [3.1. Embedding Mode (Retrieval)](#31-embedding-mode-retrieval)
  - [3.2. LLM Mode (Generation)](#32-llm-mode-generation)
- [4. Workflow](#4-workflow)
  - [4.1. Document Preparation (Offline Phase)](#41-document-preparation-offline-phase)
  - [4.2. Query Handling (Online Phase)](#42-query-handling-online-phase)
- [5. Core Components](#5-core-components)
  - [5.1. Documents & Chunking](#51-documents--chunking)
  - [5.2. Embedding Model](#52-embedding-model)
  - [5.3. Vector Database](#53-vector-database)
  - [5.4. Prompt Augmentation](#54-prompt-augmentation)
  - [5.5. Large Language Model](#55-large-language-model)
- [6. Optional Enhancements](#6-optional-enhancements)
- [7. Key Takeaways](#7-key-takeaways)
- [8. Appendix: Real-Life Analogy](#8-appendix-real-life-analogy)

---

## 1. Overview

Retrieval Augmented Generation (RAG) is a technique that improves Large Language Models (LLMs) by combining them with external knowledge sources. Normally, an LLM answers based only on what it was trained on (its memory). With RAG, the model can look up external documents in real time and use that information to give more accurate, up-to-date, and factual answers.

- **LLM = Brain** (knows general things already)
- **Embedding + Vector Database = Library** (organized for quick lookup)
- **RAG = Asking your brain to first check the library, then answer you**

**Analogy:**
- LLM is your brain.
- RAG gives your brain Google + notes library to look things up before answering.

---

## 2. Architecture

### RAG Architecture

![RAG Architecture](images/rag_architecture.png)

### Architecture at a Glance
```
            ┌───────────────────────────┐
            │   Documents / Knowledge   │
            └───────────┬───────────────┘
                        │
                Chunk + Embedding
                        │
            ┌───────────────────────────┐
            │    Vector Database (DB)   │
            └───────────┬───────────────┘
                        │
       ┌───────────────Query────────────────┐
       │                                    │
User → LLM ←────Context (Top-k docs)← Vector Search
```

---

## 3. RAG Modes

RAG operates in two main modes that work together:

### 3.1. Embedding Mode (Retrieval)
- Finds the most relevant knowledge from external sources.
- Acts as a **search engine**.
- Converts queries and documents into vectors for semantic search.

### 3.2. LLM Mode (Generation)
- Uses retrieved knowledge to generate an answer.
- Acts as the **brain** explaining results in natural language.
- Combines pre-trained memory with fresh context.

---

## 4. Workflow

### 4.1. Document Preparation (Offline Phase)
This step builds the “knowledge library” before queries are asked.

1. **Ingestion** – Collect raw content (PDFs, text files, DB rows, APIs, websites).
2. **Chunking** – Split documents into smaller passages (e.g., 300–500 tokens). Searching is easier and more precise in smaller chunks.
3. **Embedding** – Each chunk is converted into a vector (high-dimensional number array) using an embedding model (e.g., OpenAI text-embedding-3-small, Sentence-BERT). These vectors capture meaning, not just keywords. “Car” ≈ “Automobile” → stored close together. “Car” vs “Banana” → stored far apart.
4. **Storage** – Store embeddings in a Vector Database (Pinecone, Weaviate, FAISS, Milvus, etc.) with metadata (tags, document ID) and links back to the source.

### 4.2. Query Handling (Online Phase)
This step happens when a user asks a question.

#### Step A: Embedding Mode (Retrieval)
- Convert the user query into a vector using the same embedding model.
- Compare query vector to document vectors using similarity search (cosine similarity, dot product, etc.).
- Retrieve Top-k Chunks: Return the most relevant chunks (e.g., top 5) with metadata.

#### Step B: LLM Mode (Generation)
- **Prompt Construction (Context Injection):** Combine user query + retrieved chunks into a prompt template.
- Example:
  - Question: "What are the tax rules for COLA at Cognizant?"
  - Context: (chunks of retrieved documents)
- **LLM Generation:** Send the prompt to a Large Language Model (e.g., GPT-4/5, Claude, LLaMA). The LLM uses its pre-trained knowledge (general memory) and the retrieved chunks (fresh context).
- **Response:** The model generates a grounded, factual answer (often with citations).

---

## 5. Core Components

### 5.1. Documents & Chunking
Documents (PDFs, text files, web pages, etc.) are broken into chunks (small pieces of text). This makes searching easier and faster, since instead of scanning a whole book, you search a few paragraphs.

### 5.2. Embedding Model
Each chunk of text is converted into a vector (set of numbers) using an embedding model. This model could be OpenAI's text-embedding-3-small, Sentence-BERT, or similar.

- **Semantic Representation:** The embedding process captures the meaning of text, not just exact keywords. This means that words or phrases with similar meanings will have vectors that are close together in the vector space.
- **Example:**
    - “Car” and “Automobile” → similar embeddings (close in vector space).
    - “Car” vs “Banana” → very different embeddings (far apart in vector space).
- **Why is this useful?**
    - It allows the system to find relevant information even if the exact words are not used, making search and retrieval much smarter and more flexible.

Embedding models are the foundation for semantic search, enabling RAG systems to understand context and relationships between concepts, not just match keywords.

### 5.3. Vector Database
All embeddings are stored inside a vector database (like ChromaDB, Pinecone, Weaviate, FAISS, Milvus). When you ask a question (Query), it is also converted into an embedding. The system finds the closest matching chunks from the database. Example: If you ask “Who is CEO of Tesla?”, it finds chunks mentioning Elon Musk. This whole step is called Retrieval.

### 5.4. Prompt Augmentation
Retrieved chunks are added as context into a prompt template. This means that, instead of just sending your question to the LLM, you also provide extra information from relevant documents to help the model answer more accurately.

- **How it works:**
    - The system finds the most relevant pieces of information (chunks) from your knowledge base.
    - These chunks are combined with your original question to create a new, richer prompt.
    - Example prompt:
      
      "Here is some context from documents: \n[insert chunks]\n\nNow answer this question: [your question]"

- **Why is this important?**
    - The LLM gets access to up-to-date, factual, or domain-specific information that it may not have seen during training.
    - This helps the model avoid hallucinations and provide answers grounded in real data.

- **Analogy:**
    - Imagine asking a friend a question, but before they answer, you hand them a few pages of notes or articles about the topic. Their answer will be more accurate because they can reference those notes.

This process is called "augmentation" because you are augmenting (adding to) the model's knowledge with external context before it generates a response.

### 5.5. Large Language Model
The final prompt (query + context) is sent to the LLM (e.g., GPT-4, Claude, Llama). LLM uses both its pre-trained knowledge and the retrieved document chunks, then generates the final response. This step is Generation.

---

## 6. Optional Enhancements
- **Re-ranking** (cross-encoder refinement): Refine retrieved chunks with another model.
- **Caching** (faster repeated queries): Save past queries for faster answers.
- **Feedback Loops** (improve quality over time): Learn from user corrections.
- **Hybrid Search** (semantic + keyword): Mix semantic search (vectors) + keyword search (BM25).

---

## 7. Key Takeaways

- **Two Modes:**
  - Embedding Mode = Retrieval (find knowledge).
  - LLM Mode = Generation (use knowledge).
- Data is not saved into the LLM.
- LLM is not retrained.
- Knowledge remains in the vector DB and is retrieved per query.

**In short:**
RAG = Brain (LLM) + Library (Vector DB) → Smarter, factual, and up-to-date answers.

---

## 8. Appendix: Real-Life Analogy

- **LLM only** = A student answering from memory.
- **RAG** = The same student bringing a textbook + notes to the exam. They don’t rewrite their brain, but use references to give better answers.

**Expanded Analogy:**
Imagine you’re taking an exam. If you rely only on your memory, you might miss some details. But if you’re allowed to bring your textbooks and notes, you can look up facts and provide more accurate answers. RAG lets the LLM “bring its notes” to every question, making it smarter and more reliable.

---
