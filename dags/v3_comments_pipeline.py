from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from pathlib import Path
import sqlite3
import requests
import pandas as pd


"""
Variant 3 — API (comments) → SQLite → “suspicious” flags + CSV

DAG id: v3_comments_pipeline
Tasks:
  1) fetch_comments              — load comments from API into SQLite
  2) export_suspicious_comments  — flag suspicious comments and export CSV
"""

# Paths
FILE_DIR = Path(__file__).resolve().parent
DB_PATH = FILE_DIR / "v3_demo.db"
CSV_PATH = FILE_DIR / "suspicious_comments.csv"


def fetch_comments():
    """
    Task 1: Fetch comments from JSONPlaceholder API and save to SQLite.

    API: https://jsonplaceholder.typicode.com/comments
    """
    url = "https://jsonplaceholder.typicode.com/comments"
    print(f"Requesting data from {url} ...")

    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    # Keep only needed columns "id", "postId", "name", "email", "body"
    # rename postId to post_id
    df = pd.DataFrame(data)[["id", "postId", "name", "email", "body"]]
    df = df.rename(columns={"postId": "post_id"})

    # Save to SQLite database
    conn = sqlite3.connect(DB_PATH)
    df.to_sql("comments", conn, if_exists="replace", index=False)
    conn.close()

    print(f"Saved {len(df)} comments to {DB_PATH} table 'comments'.")


def export_suspicious_comments():
    """
    Task 2: Flag 'suspicious' comments and export to CSV.

    Definition of suspicious:
    - body length < 20 characters OR
    - email does not contain "@"
    """

    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("SELECT * FROM comments", conn)
    conn.close()

    print(f"Loaded {len(df)} comments from DB.")

    # condition 1: short body
    cond_short_body = df["body"].str.len() < 20

    # condition 2: invalid email
    cond_bad_email = ~df["email"].str.contains("@")

    # Combine conditions with OR
    suspicious = df[cond_short_body | cond_bad_email]

    # export to CSV
    suspicious.to_csv(CSV_PATH, index=False)

    print(
        f"Marked {len(suspicious)} comments as suspicious "
        f"and exported {len(suspicious)} rows to {CSV_PATH}."
    )


# ---------------------------
# DAG DEFINITION
# ---------------------------

with DAG(
    dag_id="v3_comments_pipeline",
    start_date=datetime.now() - timedelta(days=1),  # yesterday
    schedule_interval="0 0 * * *",  # daily at midnight
    catchup=False,
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_comments",
        python_callable=fetch_comments,
    )

    export_task = PythonOperator(
        task_id="export_suspicious_comments",
        python_callable=export_suspicious_comments,
    )

    # Dependency: first fetch, then export
    fetch_task >> export_task
