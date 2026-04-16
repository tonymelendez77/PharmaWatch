import os

from google.cloud import bigquery

from schema import FAERS_SCHEMA, PUBMED_SCHEMA, REDDIT_SCHEMA, DRUG_LABELS_SCHEMA

BQ_TYPE_MAP = {
    "VARCHAR": "STRING",
    "INTEGER": "INT64",
    "FLOAT": "FLOAT64",
    "DATE": "DATE",
    "BOOLEAN": "BOOL",
    "TIMESTAMP": "TIMESTAMP",
}


def _client():
    project_id = os.environ["GCP_PROJECT_ID"]
    return bigquery.Client(project=project_id)


def _dataset_id():
    return "{}.{}".format(os.environ["GCP_PROJECT_ID"], os.environ["BIGQUERY_DATASET"])


def ensure_dataset(client):
    dataset_ref = bigquery.Dataset(_dataset_id())
    dataset_ref.location = "US"
    client.create_dataset(dataset_ref, exists_ok=True)


def _to_schema_fields(schema_dict):
    fields = []
    for col, sql_type in schema_dict.items():
        bq_type = BQ_TYPE_MAP[sql_type.upper()]
        fields.append(bigquery.SchemaField(col, bq_type, mode="NULLABLE"))
    return fields


def load_dataframe(df, table_name, schema_dict):
    client = _client()
    ensure_dataset(client)

    table_ref = "{}.{}".format(_dataset_id(), table_name)
    job_config = bigquery.LoadJobConfig(
        schema=_to_schema_fields(schema_dict),
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    print(
        "bq: loaded {} rows -> {}.{}".format(
            len(df), os.environ["BIGQUERY_DATASET"], table_name
        )
    )


def load_all(faers_df, pubmed_df, reddit_df, labels_df):
    load_dataframe(faers_df, "faers_events", FAERS_SCHEMA)
    load_dataframe(pubmed_df, "pubmed_articles", PUBMED_SCHEMA)
    load_dataframe(reddit_df, "reddit_mentions", REDDIT_SCHEMA)
    load_dataframe(labels_df, "drug_labels", DRUG_LABELS_SCHEMA)
