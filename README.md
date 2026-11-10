# In order to run this prototype use following steps.
# Installation
1. Install python 3.6.9 or later  
2. Install following python libraries  
   pyspark  
   openai  
   psycopg  
   pypdf  
   docx  
   langchain_text_splitters  
   langchain-core  
   sentence_transformers  
   angchain_huggingface  
   langchain_ollama  
   pandas  
3. Install ollama  
   curl -fsSL https://ollama.com/install.sh | sh  
   Visit http://localhost:11434/ to check if ollama is running  

   Pull the model:  
   ollama pull llama3.2:latest  
   
4. Create a .env file in the solution directory and enter following entries in the file:  

   DATABASE_URL="postgresql://appuser:app1234@localhost:5432/demodb"  
   OLLAMA_BASE_URL="http://127.0.0.1:11434"  
   OPENAI_API_KEY="ollama"

5. Install PostgresSQL v18  
   https://www.postgresql.org/download/  

6. Setup Database demodb  
   login to PostgreSQL using psql:  

   psql -d postgres -U postgres  

   CREATE DATABASE demodb;  
   \c demodb  
   CREATE EXTENSION vector;  
   CREATE SCHEMA nordvik;  

   CREATE TABLE IF NOT EXISTS nordvik.documents (  
    id BIGSERIAL PRIMARY KEY,  
    content TEXT NOT NULL,  
    metadata JSONB,  
    embedding VECTOR(768)  
    );  

    CREATE INDEX IF NOT EXISTS docs_embedding_idx  
    ON documents  
    USING hnsw (embedding vector_cosine_ops);  
   
    CREATE USER appuser password 'app1234';  
    ALTER USER appuser SET search_path=nordvik, public;  
   
    GRANT ALL ON SCHEMA nordvik TO appuser;  
    GRANT CREATE ON DATABASE demodb TO appuser;  
7. Data cleanup:
      Raw data is stored in the directory: nordvik/bronze  
      cd nordvik/solution  
      python3 currency_scd.py	
      python3 data_cleansing.py  

      Cleaned data is stored in the directory: nordvik/silver  
      Run following python scripts to create aggregate data in nordvik/gold directory:  
      python3 build_aggregates.py  

8. Create doc embeddings and load them in PostgreSQL/pgvector  
   python3 load_docs.py  
   
# Testing
  Run the chat agent:  
      python3 nordvik_agent.py  

      The chat interface will open in a browser window.  

      Ask the questions related Nordvik's sales, open opportunities, service contracts, quarterly reviews, etc..  
      
