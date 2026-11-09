import os
import json
from pathlib import Path

import psycopg
from openai import OpenAI
from dotenv import load_dotenv
from pypdf import PdfReader
from docx import Document
from langchain_text_splitters import RecursiveCharacterTextSplitter
from sentence_transformers import SentenceTransformer
from langchain_huggingface import HuggingFaceEmbeddings

load_dotenv()

# Configuration
DATABASE_URL = os.getenv("DATABASE_URL")

DOCUMENT_DIRECTORY = "../docs"

model_name = "all-mpnet-base-v2"
model = SentenceTransformer('all-mpnet-base-v2')
CHUNK_SIZE = 1000
CHUNK_OVERLAP = 200
BATCH_SIZE = 1


# ----------------------------------
# 1. Extract document text
# ----------------------------------

def extract_text(file_path):
    extension = file_path.suffix.lower()

    if extension == ".pdf":
        reader = PdfReader(str(file_path))
        return "\n".join(
            page.extract_text() or "" for page in reader.pages
        )

    elif extension == ".docx":
        document = Document(str(file_path))
        return "\n".join(
            paragraph.text for paragraph in document.paragraphs
        )

    elif extension in [".txt", ".md"]:
        return file_path.read_text(encoding="utf-8")

    else:
        print(f"Skipping unsupported file: {file_path}")
        return None


# ----------------------------------
# 2. Split documents into chunks
# ----------------------------------

splitter = RecursiveCharacterTextSplitter(
    chunk_size=CHUNK_SIZE,
    chunk_overlap=CHUNK_OVERLAP,
    separators=["\n\n", "\n", ". ", " ", ""]
)


def process_documents(directory):
    chunks = []

    for file_path in Path(directory).rglob("*"):

        if not file_path.is_file():
            continue

        if file_path.suffix.lower() not in [
            ".pdf", ".docx", ".txt", ".md"
        ]:
            continue

        print(f"Processing: {file_path}")

        try:
            text = extract_text(file_path)

            if not text or not text.strip():
                continue

            document_chunks = splitter.split_text(text)

            for index, chunk in enumerate(document_chunks):
                chunks.append({
                    "content": chunk,
                    "metadata": {
                        "source": str(file_path),
                        "filename": file_path.name,
                        "chunk_index": index
                    }
                })

        except Exception as error:
            print(f"Error processing {file_path}: {error}")

    return chunks


# ----------------------------------
# 3. Generate embeddings
# ----------------------------------

def generate_embeddings(texts):

    embeddings = HuggingFaceEmbeddings(model_name="sentence-transformers/all-mpnet-base-v2")


    doc_embeddings = embeddings.embed_documents(texts)
 #   doc_embeddings = model.encode(texts)

    return [doc_embeddings]


# ----------------------------------
# 4. Load chunks into pgvector
# ----------------------------------

def load_into_pgvector(chunks):

    connection = psycopg.connect(DATABASE_URL)

    try:
        with connection.cursor() as cursor:

            for start in range(0, len(chunks), BATCH_SIZE):

                batch = chunks[start:start + BATCH_SIZE]

                texts = [item["content"] for item in batch]

                embeddings = generate_embeddings(texts)

                records = []

                for chunk, embedding in zip(batch, embeddings):

                    vector_string =  "".join(
                        str(value) for value in embedding
                    ) 

                    records.append((
                        chunk["content"],
                        json.dumps(chunk["metadata"]),
                        vector_string
                    ))

                cursor.executemany(
                    """
                    INSERT INTO documents
                        (content, metadata, embedding)
                    VALUES (%s, %s::jsonb, %s::vector)
                    """,
                    records
                )

                connection.commit()

                print(
                    f"Loaded {min(start + BATCH_SIZE, len(chunks))}"
                    f" / {len(chunks)} chunks"
                )

    finally:
        connection.close()


# ----------------------------------
# 5. Main execution
# ----------------------------------

def main():

    print("Starting document ingestion...")

    chunks = process_documents(DOCUMENT_DIRECTORY)

    if not chunks:
        print("No documents found.")
        return

    print(f"Total chunks created: {len(chunks)}")

    load_into_pgvector(chunks)

    print("Document ingestion completed successfully!")


if __name__ == "__main__":
    main()

