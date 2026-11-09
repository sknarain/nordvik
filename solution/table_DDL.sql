CREATE TABLE IF NOT EXISTS documents (
    id BIGSERIAL PRIMARY KEY,
    content TEXT NOT NULL,
    metadata JSONB,
    embedding VECTOR(768)
);

CREATE INDEX IF NOT EXISTS docs_embedding_idx
ON documents
USING hnsw (embedding vector_cosine_ops);


