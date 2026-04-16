import os
from datetime import date, datetime


def _resolve_auto():
    # snowflake free trial is 30 days, fall back to bq after that
    trial_start_str = os.environ["SNOWFLAKE_TRIAL_START"]
    trial_start = datetime.strptime(trial_start_str, "%Y-%m-%d").date()
    elapsed_days = (date.today() - trial_start).days
    if elapsed_days <= 30:
        chosen = "snowflake"
    else:
        chosen = "bigquery"
    print("auto: {} days since trial start, using {}".format(elapsed_days, chosen))
    return chosen


def load(faers_df, pubmed_df, reddit_df, labels_df):
    warehouse = os.environ.get("WAREHOUSE", "").lower()

    if warehouse == "auto":
        warehouse = _resolve_auto()

    if warehouse == "snowflake":
        import snowflake_loader
        snowflake_loader.load_all(faers_df, pubmed_df, reddit_df, labels_df)
    elif warehouse == "bigquery":
        import bigquery_loader
        bigquery_loader.load_all(faers_df, pubmed_df, reddit_df, labels_df)
    else:
        raise ValueError("WAREHOUSE env var must be snowflake, bigquery, or auto")

    print("loaded all tables to {}".format(warehouse))
