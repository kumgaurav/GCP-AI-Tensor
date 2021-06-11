import yaml
import pandas as pd
from google.cloud import bigquery
from pyspark.sql import SparkSession
import pyspark.sql.functions as func
from pyspark.sql.types import *

cfg_d = dict()
#f = open("config.yaml")
#cfg_d = yaml.safe_load(f)
#f.close()

#client = bigquery.Client().from_service_account_json(cfg_d["json_file_name"])

def get_full_table_name(cfg_d,table_name):
    return str("{}.{}".format(cfg_d["project_name"], table_name))

def read_view_data(spark_session, cfg_d, object_name, is_query=False):
    job_config = bigquery.QueryJobConfig()
    query_view = "SELECT * FROM `{}`".format(get_full_table_name(object_name)) if not is_query else object_name
    query_job = client.query(query=query_view, job_config=job_config)
    query_job.result()
    bq_cache = query_job.destination.to_api_repr()['projectId'] + '.' + query_job.destination.to_api_repr()[
        'datasetId'] + '.' + query_job.destination.to_api_repr()['tableId']
    print("BQ Cache Table: " + bq_cache)
    return spark_session.read.format("bigquery").option("table", bq_cache). \
        option("credentialsFile", cfg_d["json_file_name"]).load()


def write_bq_data(cfg_d, data_frame, data_set, table_name, write_mode="overwrite"):
    output_file_path = cfg_d["staging_location"] + "/" + table_name
    data_frame.write.format("bigquery").mode(write_mode). \
        option("temporaryGcsBucket", output_file_path). \
        option("project", cfg_d["project_name"]). \
        option("table", "{}.{}".format(data_set, table_name)). \
        save()
        

def write_bq_data_with_partition(cfg_d, data_frame, data_set, table_name, dateStr, write_mode="overwrite"):
    output_file_path = cfg_d["staging_location"] + "/" + table_name
    data_frame.write.format("bigquery").mode(write_mode). \
        option("temporaryGcsBucket", output_file_path). \
        option("createDisposition", "CREATE_IF_NEEDED"). \
        option("datePartition", dateStr). \
        option("partitionField", "record_ingestion_date"). \
        option("partitionType", "DAY"). \
        option("partitionExpirationMs", "15552000000"). \
        option("clusteredFields", "Name"). \
        option("project", cfg_d["project_name"]). \
        option("table", "{}.{}".format(data_set, table_name)). \
        save()