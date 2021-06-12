import pandas as pd
import yfinance as yf
import datetime as dt
import pytz
import time
import requests
import io
import os,ssl,sys
from pytz import timezone
from absl import app
from absl import flags
from bq_utils import *
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType
from google.cloud import bigquery
from collections import OrderedDict

flags.DEFINE_string(
    'config', None,
    'Config file.')

FLAGS = flags.FLAGS
DATE_FORMAT = "yyyyMMdd"

def main(argv):
    if not args_checks(FLAGS):
        return
    parsed_path = os.path.abspath(FLAGS.config)
    #print("parsed_path ->"+str(parsed_path))
    cfg_d = dict()
    f = open(parsed_path)
    cfg_d = yaml.safe_load(f)
    #print (cfg_d)
    utc_now = pytz.utc.localize(dt.datetime.utcnow())
    pst_date = utc_now.astimezone(timezone('US/Pacific'))
    # Input Start and End Date
    start = pst_date - dt.timedelta(days=30)
    end = pst_date #dt.datetime(2021,6,10)
    print("start -> "+str(start))
    print("end  -> "+str(end))
    start_date_str=start.strftime("%Y%m%d")
    end_date_str=end.strftime("%Y%m%d")
    partition_date_str = '20210513'#start_date_str
    print("start date -> "+start_date_str)
    print("end date -> "+end_date_str)
    print("partition_date_str -> "+partition_date_str)
    conf = SparkConf()
    #conf.set("spark.sql.execution.arrow.enabled", "true")
    spark = SparkSession.builder.appName('symbols_by_close').config(conf=conf).getOrCreate()
    df = spark.read \
        .format('bigquery') \
        .load('bigquery-public-data.samples.shakespeare')
    df.show(5)    
    
def args_checks(FLAGS):
    if not FLAGS.config:
        print("Error: Must define  config file.")
        return False
    return True


if __name__ == '__main__':
    app.run(main)