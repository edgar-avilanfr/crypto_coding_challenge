# Copyright 2023 Google LLC

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#cron generator web: https://crontab.guru/#4_5_*_1-12_1-5

import os
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    ClusterGenerator,
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator
)

from airflow.providers.google.cloud.operators.bigquery import  BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator
from datetime import datetime


gcp_project_id = os.environ.get('GCP_PROJECT_ID')
gcs_source_data_bucket = os.environ.get('GCS_SOURCE_DATA_BUCKET')
REGION = 'us-central1'
logical_date_nodash= '{{ds_nodash}}'
logical_date= '{{ds}}'
CLUSTER_NAME = 'cluster-smallxs2-{{ ds_nodash }}'
PYSPARK_URI1= f"gs://{gcs_source_data_bucket}/crypto_coins_code/01_crypto_ingest_rt.py"
PYSPARK_URI2= f"gs://{gcs_source_data_bucket}/crypto_coins_code/02_crypto_transform_1.py"
DATASET_NAME= os.environ.get('DATASET_NAME')
COIN_LIST= os.environ.get('COIN_LIST')
COIN_API_KEY= os.environ.get('COIN_API_KEY')


PYSPARK_JOB1 = {
    "reference": {"project_id": gcp_project_id},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI1,
                    "jar_file_uris": ["gs://spark-lib/bigquery/spark-3.4-bigquery-0.41.1.jar"],
                    "properties": {"spark.executorEnv.mydate":logical_date,
                                   "spark.executorEnv.coin_list":COIN_LIST,
                                   "spark.executorEnv.api_key": COIN_API_KEY
                    }}
}


def _check_cluster_config():
    print(f"Cluster config is: {cluster_config_args}")
    print(type(logical_date_nodash))

cluster_config_args= ClusterGenerator(
    project_id = gcp_project_id,
    master_machine_type= "n2-standard-4",
    num_workers=0,
    master_disk_size=37,
    idle_delete_ttl=480, # in seconds
    internal_ip_only= False
).make()

ui_args = {
    'owner': 'Edgar Avilan',
    'project': 'Crypto_processing'
}

with DAG(
    dag_id= 'crypto_pipeline_daily',
    schedule= '1 * * * *',
    start_date= datetime.now(),
    max_active_runs=1,
    default_args= ui_args
) as dag:
    
    get_cluster_results= PythonOperator(
        task_id= "print_oss",
        python_callable= _check_cluster_config
    )


    create_cluster= DataprocCreateClusterOperator(
        task_id= '1_create_clusters',
        project_id = gcp_project_id,
        cluster_config= cluster_config_args,
        region= REGION,
        cluster_name = CLUSTER_NAME,
        gcp_conn_id= "gcp_conn",
        
    )

    submit_job= DataprocSubmitJobOperator(
        task_id= '2_submit_jobs',
        job= PYSPARK_JOB1,
        region= REGION,
        project_id = gcp_project_id,
        gcp_conn_id= "gcp_conn"

    )

    delete_cluster= DataprocDeleteClusterOperator(
        task_id = '3_delete_clusters',
        region= REGION,
        project_id = gcp_project_id,
        cluster_name = CLUSTER_NAME,
        gcp_conn_id= "gcp_conn",
    )

    upsert_task2= BigQueryInsertJobOperator(
        task_id='upsert_data2',
        configuration={
            "query": {
                "query": """
                    MERGE INTO developer3-457903.local_dev.crypto_lz_dwh AS T
                    USING developer3-457903.local_dev.crypto_lz_RT AS S
                    ON T.DATE = S.DATE AND T.ID = S.ID
                    WHEN NOT MATCHED THEN
                        INSERT (date, price, ID )
                        VALUES (S.date, S.price, S.ID)
                """,
                "useLegacySql": False,
            }
        },
        gcp_conn_id= "gcp_conn" 
    )

    truncate_table= BigQueryInsertJobOperator(
        task_id='truncate_lz',
        configuration={
            "query": {
                "query": """
                    truncate table developer3-457903.local_dev.crypto_lz_RT
                """,
                "useLegacySql": False,
            }
        },
        gcp_conn_id= "gcp_conn" 
    )

[get_cluster_results] >>  create_cluster >> submit_job >> [delete_cluster,upsert_task2] >> truncate_table