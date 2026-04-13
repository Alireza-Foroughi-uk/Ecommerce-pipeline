# E-Commerce Data Pipeline 🛒
An end-to-end data engineering pipeline that generates, processes, and analyses e-commerce sales data using Apache Airflow, AWS S3, AWS Glue, and AWS Athena.

## Architecture 🏗️
![Pipeline Architecture](pipeline_architecture_review.svg)

## Tech Stack 🛠️
    - **Orchestration:** Apache Airflow 2.8 (AWS EC2)
    - **Storage:** AWS S3
    - **Catalogue & Crawling:** AWS Glue
    - **Querying:** AWS Athena
    - **Language:** Python 3.10
    - **Libraries:** Pandas, Boto3, Faker

## Pipeline Steps 🔄
    1. ⚙️ **Generate** — Faker generates 10,000 realistic UK e-commerce orders and uploads raw CSV to S3
    2. 🧹 **Transform** — Cleans data, removes cancelled/refunded orders, adds revenue and date columns, saves to S3 processed folder
    3. 🔍 **Crawl** — AWS Glue crawler scans the processed folder and updates the data catalogue
    4. 📊 **Query** — AWS Athena queries clean data directly from S3 using SQL

## DAG Structure 🔀 
    generate_and_upload → transform_and_upload → run_glue_crawler

## Key Results 📈 

    - 10,000 orders generated daily across 10 product categories
    - 8,400+ completed and pending orders after transformation
    - £1M+ monthly revenue tracked across 13 months
    - Top product: Monitor at £1.3M total revenue

## Project Structure 📁 📁 📁 

```
ecommerce-pipeline/
├── dags/
│   └── ecommerce_pipeline.py
├── scripts/
│   ├── generate_orders.py
│   ├── transform.py
│   └── upload_to_s3.py
└── README.md
```

## Setup ⚙️ 

    1. Clone the repo
    2. Install dependencies: `pip install apache-airflow boto3 pandas faker`
    3. Add your AWS credentials to each script
    4. Copy `dags/ecommerce_pipeline.py` to your Airflow DAGs folder
    5. Trigger the DAG from the Airflow UI

## AWS Services Used ☁️ 

| Service | Purpose |
|---|---|
| EC2 | Hosts Apache Airflow |
| S3 | Stores raw and processed data |
| Glue | Crawls S3 and creates data catalogue |
| Athena | SQL queries on S3 data |

## Author 👤 

Alireza Foroughi — Junior Data Engineer
[LinkedIn](https://linkedin.com/in/alirezaforoughi) | [GitHub](https://github.com/Alireza-Foroughi-uk)
