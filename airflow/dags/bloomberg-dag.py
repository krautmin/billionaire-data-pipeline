import datetime
import json
import pendulum
import polars as pl
import os
import re

import requests
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator


@dag(
    dag_id="bloomberg-data",
    schedule_interval="0 1 * * *",
    start_date=pendulum.datetime(2023, 1, 25, tz="Europe/Berlin"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=5),
)
def BloombergDAG():
    create_bloomberg_table = PostgresOperator(
        task_id="create_bloomberg_table",
        postgres_conn_id="postgres_default",
        sql="""
            CREATE TABLE IF NOT EXISTS bloomberg (
                "Name" TEXT,
                "Country" TEXT,
                "Industry" TEXT,
                "PersonId" INTEGER,
                "Rank" INTEGER,
                "NetWorth" INTEGER,
                "YTDChange" INTEGER,
                "Sector" TEXT,
                "Timestamp" DATE
            );""",
    )

    create_bloomberg_temp_table = PostgresOperator(
        task_id="create_bloomberg_temp_table",
        postgres_conn_id="postgres_default",
        sql="""
            DROP TABLE IF EXISTS bloomberg_temp;
            CREATE TABLE bloomberg_temp (
                "Name" TEXT,
                "Country" TEXT,
                "Industry" TEXT,
                "PersonId" INTEGER,
                "Rank" INTEGER,
                "NetWorth" INTEGER,
                "YTDChange" INTEGER,
                "Sector" TEXT,
                "Timestamp" DATE
            );""",
    )

    @task
    def get_data():
        now = datetime.now()
        data_path = f"/root/airflow/data/bloomberg/{now.strftime('%Y')}/bloomberg-{now.strftime('%Y-%m-%d-%H-%M-%S')}.feather"
        os.makedirs(os.path.dirname(data_path), exist_ok=True)

        data_bloomberg = requests.get(
            'https://www.bloomberg.com/billionaires/',
            headers={
                'User-Agent':
                    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.1 Safari/605.1.15'
            }
        )

        json_list = [json.loads(row) for row in re.findall(r"{.*?}", data_bloomberg.text) if "commonName" in row]

        df = pl.from_dicts(json_list) \
            .select(
            ['commonName', 'commonLastName', 'citizenship', 'industry', 'personId', 'rank', 'worth', 'ytdChange',
             'sector']) \
            .rename(
            {'commonName': "Name", 'commonLastName': "LastName", 'citizenship': "Country", 'industry': "Industry",
             'personId': "PersonId", 'rank': "Rank", 'worth': "NetWorth", 'ytdChange': "YTDChange", 'sector': "Sector"}) \
            .with_column(pl.lit(now).alias('Timestamp'))
        
        df.write_ipc(data_path, compression="lz4")

        postgres_hook = PostgresHook(postgres_conn_id="postgres_default")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        with open(data_path, "r") as file:
            cur.copy_expert(
                "COPY employees_temp FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
                file,
            )
        conn.commit()

    @task
    def merge_data():
        query = """
            INSERT INTO employees
            SELECT *
            FROM (
                SELECT DISTINCT *
                FROM employees_temp
            ) t
            ON CONFLICT ("Serial Number") DO UPDATE
            SET "Serial Number" = excluded."Serial Number";
        """
        try:
            postgres_hook = PostgresHook(postgres_conn_id="tutorial_pg_conn")
            conn = postgres_hook.get_conn()
            cur = conn.cursor()
            cur.execute(query)
            conn.commit()
            return 0
        except Exception as e:
            return 1

    [create_bloomberg_table, create_bloomberg_temp_table] >> get_data() >> merge_data()


dag = BloombergDAG()