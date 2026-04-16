import os

import chromadb
from sentence_transformers import SentenceTransformer

EMBEDDING_MODEL = "all-MiniLM-L6-v2"
COLLECTION_NAME = "pharmawatch_pubmed"

_model = None
_client = None
_collection = None


def _get_model():
    global _model
    if _model is None:
        _model = SentenceTransformer(EMBEDDING_MODEL)
    return _model


def _get_collection():
    global _client, _collection
    if _collection is None:
        persist_dir = os.environ["CHROMA_PERSIST_DIR"]
        _client = chromadb.PersistentClient(path=persist_dir)
        _collection = _client.get_or_create_collection(name=COLLECTION_NAME)
    return _collection


def retrieve(query, drug_name, n_results=5):
    model = _get_model()
    collection = _get_collection()

    embedding = model.encode([query]).tolist()

    where_filter = {"drug_name": drug_name} if drug_name else None

    results = collection.query(
        query_embeddings=embedding,
        n_results=n_results,
        where=where_filter,
    )

    ids = (results.get("ids") or [[]])[0]
    documents = (results.get("documents") or [[]])[0]
    metadatas = (results.get("metadatas") or [[]])[0]
    distances = (results.get("distances") or [[]])[0]

    out = []
    for i in range(len(ids)):
        meta = metadatas[i] if i < len(metadatas) else {}
        distance = distances[i] if i < len(distances) else 0.0
        similarity = 1.0 - float(distance)
        out.append({
            "article_id": meta.get("article_id", ids[i]),
            "title": meta.get("title", ""),
            "abstract": documents[i] if i < len(documents) else "",
            "drug_name": meta.get("drug_name", ""),
            "publish_year": meta.get("publish_year", 0),
            "similarity_score": similarity,
        })

    return out


def retrieve_for_profile(drug_name, age_group, risk_label, n_results=5):
    query = "{} adverse effects {} patients {} risk".format(drug_name, age_group, risk_label)
    return retrieve(query, drug_name, n_results)
