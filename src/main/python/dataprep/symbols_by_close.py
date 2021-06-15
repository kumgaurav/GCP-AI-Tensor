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
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType
from functools import reduce
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
    start = pst_date - dt.timedelta(days=1)
    end = pst_date #dt.datetime(2021,6,10)
    print("start -> "+str(start))
    print("end  -> "+str(end))
    start_date_str=start.strftime("%Y%m%d")
    end_date_str=end.strftime("%Y%m%d")
    partition_date_str = end_date_str
    print("start date -> "+start_date_str)
    print("end date -> "+end_date_str)
    print("partition_date_str -> "+partition_date_str)
    Symbols=cfg_d["symbols"].split(",")
    Symbols.sort()
    list(OrderedDict.fromkeys(Symbols))
    print("companies -> "+str(Symbols))
    conf = SparkConf()
    conf.set("viewsEnabled", "true")
    conf.set("materializationDataset", "janus_graph")
    conf.set("materializationProject", "itd-aia-datalake")
    spark = SparkSession.builder.appName('symbols_by_close').config(conf=conf).getOrCreate()
    t0 = time.time()

    # create empty dataframe
    stock_final = pd.DataFrame()
    
    # iterate over each symbol
    for i in Symbols:  
        
        # print the symbol which is being downloaded
        #print( str(Symbols.index(i)) + str(' : ') + i, sep=',', end=',', flush=True)  
        
        try:
            # download the stock price 
            stock = []
            stock = yf.download(i,start=start, end=end, progress=False)
            
            # append the individual stock prices 
            if len(stock) == 0:
                None
            else:
                stock['Name']=i.strip()
                stock_final = stock_final.append(stock,sort=False)
        except Exception:
            None
            
    t1 = time.time()
    
    total = t1-t0
    stock_final.Name.unique()
    #print("stock_final.index.names -> "+str(stock_final.index.names))
    #print("stock_final.columns.names -> "+str(stock_final.columns.names))
    #print("stock_final.dtypes "+str(stock_final.dtypes))
    #print("stock_final.axes "+str(stock_final.axes))
    #print("stock_final.ndim "+str(stock_final.ndim))
    #print("stock_final.shape "+str(stock_final.shape))
    #print("stock_final.shape "+str(stock_final.shape))
    #stock_final.to_csv('stock_final_11Oct2020.csv')
    #print(stock_final.head(10))
    stock_final.reset_index(inplace=True)
    #print("df.axes "+str(stock_final.axes))
    #print("df.ndim "+str(stock_final.ndim))
    #print(stock_final.head(10))
    df1 = spark.createDataFrame(stock_final)
    df1 = df1.dropDuplicates(['Date','Name']).filter(isnan(col("Open")) != True)
    df1 = df1.withColumnRenamed("Adj Close", "AdjClose").withColumn("record_ingestion_date",to_date(lit(partition_date_str),DATE_FORMAT)).withColumn("Date",to_date(col("Date")))
    #sparkDF.show(5)
    #sparkDF.printSchema()
    df1 = df1.withColumn("Open",bround(col("Open"),2)).withColumn("High",bround(col("High"),2)).withColumn("Low",bround(col("Low"),2)).withColumn("Close",bround(col("Close"),2)).withColumn("AdjClose",bround(col("AdjClose"),2)).withColumn("Volume",bround(col("Volume").cast(DoubleType()),2))
    df1 = df1.select("Name","Date","Open","Close","Low","High","AdjClose","Volume","record_ingestion_date")
    sql = """
      SELECT Name,Date,Open,Close,Low,High,AdjClose,Volume,record_ingestion_date 
      FROM  `itd-aia-datalake.janus_graph.symbols_by_close` a
      where record_ingestion_date=(Select max(record_ingestion_date) FROM `itd-aia-datalake.janus_graph.symbols_by_close`);
      """
    df2 = spark.read.format("bigquery").option("query", sql).load()
    record_ingestion_date = df2.first()['record_ingestion_date']
    print("record_ingestion_date -> "+str(record_ingestion_date))
    #df2.show()
    # create list of dataframes
    dfs = [df1, df2]

    # create merged dataframe
    sparkDF = reduce(DataFrame.unionAll, dfs)
    sparkDF.schema['record_ingestion_date'].nullable = False
    sparkDF.printSchema()
    sparkDF.show()
    my_window = Window.partitionBy("Name").orderBy("Date")

    sparkDF = sparkDF.withColumn("prev_close", lag(sparkDF.Close).over(my_window))
    sparkDF = sparkDF.withColumn("change", when(isnull(sparkDF.Close - sparkDF.prev_close), 0).otherwise(round((sparkDF.Close - sparkDF.prev_close),2)))
    sparkDF = sparkDF.withColumn("change_in_percent",round((sparkDF.change/sparkDF.prev_close)*100,2))
    #sparkDF.show(5)
    sparkDF=sparkDF.filter(col("record_ingestion_date")!=record_ingestion_date).withColumn("record_ingestion_date",to_date(lit(partition_date_str),DATE_FORMAT))
    sparkDF.show()
    #sparkDF.repartition(1).write.mode('overwrite').format("csv").option("header","true").save("/Users/gkumargaur/tmp/sap")
    write_bq_data_with_partition(cfg_d,sparkDF,cfg_d['data_set'], "symbols_by_close", partition_date_str, write_mode="overwrite")
    
    
def args_checks(FLAGS):
    if not FLAGS.config:
        print("Error: Must define  config file.")
        return False
    return True


if __name__ == '__main__':
    app.run(main)