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
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator



gcp_project_id = os.environ.get('GCP_PROJECT_ID')
gcs_source_data_bucket = os.environ.get('GCS_SOURCE_DATA_BUCKET')
REGION = 'us-central1'
logical_date_nodash= '{{ds_nodash}}'
CLUSTER_NAME = 'forstocks-from-local3-{{ ds_nodash }}'
PYSPARK_URI= f"gs://{gcs_source_data_bucket}/dataproc_jobs/simple_stock_flow_daily.py"
DATASET_NAME= "local_dev_stocks"

init_actions = ['gs://advanced-de-flows8-bucket/dataproc_jobs/init_scripts/pip-install.sh']


PYSPARK_JOB = {
    "reference": {"project_id": gcp_project_id},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI,
                    "jar_file_uris": ["gs://spark-lib/bigquery/spark-3.4-bigquery-0.41.1.jar"],
                    "properties": {"spark.executorEnv.mydate":logical_date_nodash,
                                   "spark.executorEnv.days_minus":"8",
                                   "spark.executorEnv.days_more":"2"}
                    }
}


def _check_cluster_config():
    print(f"Cluster config is: {cluster_config_args}")
    print(type(logical_date_nodash))

cluster_config_args= ClusterGenerator(
    project_id = gcp_project_id,
    master_machine_type= "n2-standard-4",
    num_workers=0,
    master_disk_size=37,
    idle_delete_ttl=800, # in seconds
    internal_ip_only= False
).make()

ui_args = {
    'owner': 'Edgar Avilan',
    'project': 'Logs_processing'
}

with DAG(
    dag_id= 'stock_pipeline_daily',
    schedule_interval= '08 10 * * *',
    start_date= days_ago(1),
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
        gcp_conn_id= "gcp-conn-2",
        
    )

    submit_job= DataprocSubmitJobOperator(
        task_id= '2_submit_jobs',
        job= PYSPARK_JOB,
        region= REGION,
        project_id = gcp_project_id,
        gcp_conn_id= "gcp-conn-2"

    )

    delete_cluster= DataprocDeleteClusterOperator(
        task_id = '3_delete_clusters',
        region= REGION,
        project_id = gcp_project_id,
        cluster_name = CLUSTER_NAME,
        gcp_conn_id= "gcp-conn-2",
    )

    upsert_task2= BigQueryInsertJobOperator(
        task_id='upsert_data2',
        configuration={
            "query": {
                "query": """
                    MERGE INTO advanced-de-flows8.local_dev_stocks.stocks_dwh AS T
                    USING advanced-de-flows8.local_dev_stocks.stocks_lz AS S
                    ON T.DATE = S.DATE AND T.Ticker = S.Ticker
                    WHEN MATCHED THEN
                        UPDATE SET T.Ticker=S.Ticker
                    WHEN NOT MATCHED THEN
                        INSERT (CLOSE, LOW, DATE, Ticker, CLOSE_MXN,LOW_MXN, CLOSE_dif, CLOSE_MXN_dif )
                        VALUES (S.CLOSE, S.LOW, S.DATE, S.Ticker, S.CLOSE_MXN,LOW_MXN, S.CLOSE_dif, S.CLOSE_MXN_dif)
                """,
                "useLegacySql": False,
            }
        },
        gcp_conn_id= "gcp-conn-2" 
    )

    truncate_table= BigQueryInsertJobOperator(
        task_id='truncate_lz',
        configuration={
            "query": {
                "query": """
                    truncate table advanced-de-flows8.local_dev_stocks.stocks_lz
                """,
                "useLegacySql": False,
            }
        },
        gcp_conn_id= "gcp-conn-2" 
    )

[get_cluster_results] >>  create_cluster >> submit_job >> [delete_cluster,upsert_task2] >> truncate_table