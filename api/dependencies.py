import os
from dataclasses import dataclass
from typing import Any, Optional

import pandas as pd


@dataclass
class AppState:
    master_df: Any
    reddit_df: Any
    pubmed_df: Any
    labels_df: Any
    agent: Any


app_state: Optional[AppState] = None


def _load_snowflake_table(table_name):
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
            cursor.execute("SELECT * FROM {}".format(table_name))
            rows = cursor.fetchall()
            columns = [d[0].lower() for d in cursor.description]
            df = pd.DataFrame(rows, columns=columns)
        finally:
            cursor.close()
    finally:
        conn.close()
    return df


def _load_bigquery_table(table_name):
    from google.cloud import bigquery
    client = bigquery.Client(project=os.environ["GCP_PROJECT_ID"])
    fq_table = "{}.{}.{}".format(
        os.environ["GCP_PROJECT_ID"],
        os.environ["BIGQUERY_DATASET"],
        table_name,
    )
    df = client.query("SELECT * FROM `{}`".format(fq_table)).to_dataframe()
    df.columns = [c.lower() for c in df.columns]
    return df


def _load_table(table_name):
    warehouse = os.environ.get("WAREHOUSE", "").lower()
    if warehouse == "snowflake":
        return _load_snowflake_table(table_name)
    if warehouse == "bigquery":
        return _load_bigquery_table(table_name)
    raise ValueError("WAREHOUSE must be 'snowflake' or 'bigquery'")


def load_app_state():
    master_df = _load_table("drug_master_profile")
    reddit_df = _load_table("reddit_mentions")
    pubmed_df = _load_table("pubmed_articles")
    labels_df = _load_table("drug_labels")

    from agent import build_agent
    agent = build_agent(
        master_df=master_df,
        reddit_df=reddit_df,
        pubmed_df=pubmed_df,
        labels_df=labels_df,
    )

    return AppState(
        master_df=master_df,
        reddit_df=reddit_df,
        pubmed_df=pubmed_df,
        labels_df=labels_df,
        agent=agent,
    )


def get_app_state():
    return app_state
