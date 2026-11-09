import os
from pathlib import Path
from langchain_core.tools import tool
from dotenv import load_dotenv
import pandas as pd
import psycopg
from sentence_transformers import SentenceTransformer
from langchain_huggingface import HuggingFaceEmbeddings

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
OLLAMA_BASE_URL=os.getenv("OLLAMA_BASE_URL")
OPENAI_API_KEY=os.getenv("OPENAI_API_KEY")

@tool
def get_accounts_data() -> str:
     """Read a CSV file to get aggregate data,"""
     agg_data_path="../gold/aggregates"
     all_files = Path(agg_data_path).rglob('*.csv')
     df = pd.concat((pd.read_csv(file) for file in all_files), ignore_index=True)
     return df.to_string()

@tool
def search_docs(query: str) -> str:
    """Search document embeddings for user query,"""
    #model = HuggingFaceEmbeddings(model_name="sentence-transformers/all-MiniLM-L6-v2")
    model = SentenceTransformer("sentence-transformers/all-mpnet-base-v2")
    query_embedding = model.encode(query).tolist()
    #query_embedding = str(model.embed_documents(query))
    connection = psycopg.connect(DATABASE_URL)
    result = ""
    try:
       with connection.cursor() as cursor:
             cursor.execute(
             """
             SELECT content, metadata, embedding <=> %s::vector AS distance 
             FROM documents 
             ORDER BY distance ASC 
             LIMIT 1;
             """,
             (query_embedding,)
             )  

             row = cursor.fetchone()
             if row:
                return(row)
                #print(f"\nClosest Match Found (ID: {row[0]}):")
                #print(f"Content: {row[1]}")
                #print(f"Cosine Distance: {row[2]:.4f}")
    finally:
          connection.close()
    

@tool
def below_target_margin_contracts() -> str:
     """Read a CSV file to get aggregate data,"""
     agg_data_path="../gold/below_margin_contracts"
     all_files = Path(agg_data_path).rglob('*.csv')
     df = pd.concat((pd.read_csv(file) for file in all_files), ignore_index=True)
     return df.to_string()

