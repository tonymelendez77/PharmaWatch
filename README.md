# PharmaWatch

Drug risk intelligence platform. Give it a drug name + your age/weight and it spits out risk scores, side effects, a research digest, and a chat agent for follow-up questions.

## How it works

You enter a drug and a basic profile. The system pulls adverse event data from FDA FAERS, papers from PubMed, drug labels from OpenFDA, and patient discussion from Reddit. Four LightGBM models score risk across serious reactions, hospitalization, death, and disability. SHAP explains what's driving the serious reaction score. A RAG agent (Groq + Llama 3) handles follow-up questions grounded in the research.

## Architecture

Terraform sets up AWS, GCP, and Snowflake. Reddit streams through Kafka into S3. FAERS, PubMed, and OpenFDA come in through Airflow DAGs. Great Expectations validates each batch before anything moves forward. Iceberg manages table state on S3 via Glue Catalog. Scala Spark jobs on Databricks do the heavy cleaning/enrichment. Data goes into Snowflake or BigQuery depending on trial status (auto-switches after 30 days). dbt builds the mart layer — the key table is `drug_master_profile` which joins everything per drug. LightGBM trains four classifiers tracked in MLflow. ChromaDB indexes PubMed abstracts, LangChain wires retrieval to Groq for the agent. FastAPI serves the backend from Render, Gradio frontend lives on HuggingFace Spaces.

## Tech stack

| Layer | Tools |
|-------|-------|
| Infra | Terraform, AWS S3, IAM, Glue Catalog, GCP BigQuery, Snowflake |
| Ingestion | Confluent Kafka, Airflow, Reddit API, FDA FAERS, PubMed/NCBI, OpenFDA |
| Storage | Apache Iceberg on S3 |
| Quality | Great Expectations |
| Transform | Spark (Scala), dbt |
| Compute | Databricks |
| ML | LightGBM, MLflow, SHAP |
| RAG | ChromaDB, SentenceTransformers, LangChain, Groq/Llama 3 |
| Serving | FastAPI (Render), Gradio (HuggingFace Spaces) |

## Project layout

```
terraform/       infra-as-code (AWS, GCP, Snowflake)
iceberg/         lake table setup
kafka/           reddit streaming (producer + consumer)
airflow/         batch ingestion DAGs
quality/         great expectations suites
spark/           scala transformers (Databricks)
warehouse/       snowflake + bigquery loaders
dbt/             sql transforms, mart layer
ml/              lightgbm training, prediction, shap
rag/             chromadb + langchain + groq agent
api/             fastapi backend
ui/              gradio frontend
```

## Why these choices

**Python + Scala + SQL + HCL** — each where it makes sense. Scala for Spark because type safety catches schema issues at compile time. Python for ML/API/orchestration. SQL for dbt.

**Kafka for Reddit, Airflow for the rest** — Reddit is continuous and bursty. FAERS/PubMed/OpenFDA update daily at best, so batch is fine.

**Switchable warehouse** — Snowflake trial expires after 30 days. Set `WAREHOUSE=auto` and it falls back to BigQuery. No code changes needed.

**Two quality layers** — Airflow filters bad records before writing to S3, then a separate GE checkpoint downloads the file and runs the full suite. If the checkpoint fails, the DAG stops.

**SHAP on serious model only** — it's expensive. The serious reaction score is what users look at first. The other three models just report probabilities.

## Demo

- UI: _HuggingFace Spaces URL_
- API: _Render URL_/docs

## Author

Oscar Melendez — [github.com/tonymelendez77](https://github.com/tonymelendez77)
