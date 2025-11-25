project/
 ├── etl/
 │    ├── extract.py
 │    ├── transform.py
 │    └── load.py
 ├── utils/
 │    └── file_loader.py
 ├── dags/
 │    └── etl_pipeline.py
 ├── data/
 │    └── raw/events.csv
 ├── warehouse.db
 └── README.md
Pipeline Flow
Extract → Transform → Load → SQLite Warehouse → (Optional: BI Dashboard)

1️⃣ Extract

Load CSV from S3 path
If S3 not available → fallback to local file
Clean column names & remove duplicates

2️⃣ Transform

Clean and format timestamps
Create event_date
Normalize event names
Fill missing values
Produce clean DataFrame

3️⃣ Load

Write to SQLite database (warehouse.db)

Table name: fact_events

4️⃣ Airflow DAG

Runs daily and executes:

extract.py → transform.py → load.py

⚙️ Tech Stack
Component	Tool
Language	Python
Orchestration	Airflow
Data Warehouse	SQLite
Storage	S3 / Local
Dependencies	pandas, sqlalchemy, boto3
