import os

import pandas as pd
from sentence_transformers import SentenceTransformer
import chromadb

EMBEDDING_MODEL = "all-MiniLM-L6-v2"
COLLECTION_NAME = "pharmawatch_pubmed"


def load_pubmed_from_warehouse():
    warehouse = os.environ.get("WAREHOUSE", "").lower()
    if warehouse == "snowflake":
        import snowflake.connector
        conn = snowflake.connector.connect(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            password=os.environ["SNOWFLAKE_PASSWORD"],
            database=os.environ["SNOWFLAKE_DATABASE"],
            schema=os.environ["SNOWFLAKE_SCHEMA"],
            warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        )
        try:
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT * FROM pubmed_articles")
                rows = cursor.fetchall()
                columns = [d[0].lower() for d in cursor.description]
                df = pd.DataFrame(rows, columns=columns)
            finally:
                cursor.close()
        finally:
            conn.close()
        return df
    if warehouse == "bigquery":
        from google.cloud import bigquery
        client = bigquery.Client(project=os.environ["GCP_PROJECT_ID"])
        table = "{}.{}.pubmed_articles".format(
            os.environ["GCP_PROJECT_ID"], os.environ["BIGQUERY_DATASET"]
        )
        df = client.query("SELECT * FROM `{}`".format(table)).to_dataframe()
        df.columns = [c.lower() for c in df.columns]
        return df
    raise ValueError("WAREHOUSE must be 'snowflake' or 'bigquery'")


def main():
    persist_dir = os.environ["CHROMA_PERSIST_DIR"]
    client = chromadb.PersistentClient(path=persist_dir)
    collection = client.get_or_create_collection(name=COLLECTION_NAME)

    model = SentenceTransformer(EMBEDDING_MODEL)
    df = load_pubmed_from_warehouse()

    df = df.dropna(subset=["abstract", "article_id"])
    df = df[df["abstract"].astype(str).str.len() > 0]

    if df.empty:
        print("no articles to embed")
        return

    abstracts = df["abstract"].astype(str).tolist()
    ids = df["article_id"].astype(str).tolist()
    embeddings = model.encode(abstracts, show_progress_bar=False).tolist()

    metadatas = []
    for _, row in df.iterrows():
        publish_year = row.get("publish_year")
        if publish_year is None or (isinstance(publish_year, float) and pd.isna(publish_year)):
            publish_year_val = 0
        else:
            try:
                publish_year_val = int(publish_year)
            except (TypeError, ValueError):
                publish_year_val = 0

        metadatas.append({
            "article_id": str(row.get("article_id") or ""),
            "drug_name": str(row.get("drug_name") or ""),
            "title": str(row.get("title") or ""),
            "publish_year": publish_year_val,
        })

    collection.upsert(
        ids=ids,
        documents=abstracts,
        embeddings=embeddings,
        metadatas=metadatas,
    )

    print("embedded {} articles".format(len(ids)))


if __name__ == "__main__":
    main()
