##### lib
from pyspark import SparkContext 
from pyspark.sql import SparkSession,SQLContext,Row,DataFrame
from pyspark.sql import functions as F
from pyspark.sql import DataFrameStatFunctions as SF
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime, timedelta, date
import calendar
from functools import reduce
import numpy as np
import pandas as pd
import argparse
import json
# import requests
import os
# from haversine import haversine, Unit
from math import radians, cos, sin, asin, sqrt,floor,ceil
# shell : default SparkSession available as 'spark',sc - context ,use below for submit mode:
spark = SparkSession.builder.appName('arpan_attr_panel_v0_master').getOrCreate()
sc = spark.sparkContext
sqlContext = SQLContext(sc)
# sc.setLogLevel("WARN")


#### fetch staypoint data for a day :parquet
# to do : add logic for any of rxmode/rtb/cubiq or any combination of sources
# may need to add checks for if exists for ech path : or country based path list
def get_one_day_sp_dataframe(date_1, country, spark = spark):
    base_path = "s3://near-datamart/staypoints/"
    end_path = "year=%s/month=%s/day=%s/country=%s/*"%(date_1.year,'{:02d}'.format(date_1.month),'{:02d}'.format(date_1.day),country)
    ## paths
    # sources = ["xmode", "rtb", "cuebiq" ]
    path_date_xmode = base_path + "xmode/" + end_path
    path_date_rtb = base_path + "rtb/" + end_path
    path_date_cub = base_path + "cuebiq/" + end_path
    ## read one by one
    df_x = spark.read.parquet(path_date_xmode)
    df_x = df_x.selectExpr("ifa as ifa","eventTs as eventTs","geoHash9 as geoHash9")
    df_r = spark.read.parquet(path_date_rtb)
    df_r = df_r.selectExpr("ifa as ifa","eventTs as eventTs","geoHash9 as geoHash9")
    df_c = spark.read.parquet(path_date_cub)
    df_c = df_c.selectExpr("ifa as ifa","eventTs as eventTs","geoHash9 as geoHash9")
    ## union df
    dfs = [df_x, df_r, df_c]
    # dfs = [df_x,df_r]
    df = reduce(DataFrame.unionAll, dfs) 
    
    # gh7 from gh9
    df = df.withColumn("geoHash7", F.expr("substring(geoHash9,1,length(geoHash9)-2 )")).drop("geoHash9")
    df = df.repartition(200)
    ## note : key = ifa + eventTs
    return df

#### create time index as a new col:
# df should comtain col "eventTs"
# index_hrs_day could be varied as num of hrs 
def add_index_eventTs(date_1, df, index_hrs_day):
    ## date to ts : midnight :  timegm doing as default
    date_1_ts = calendar.timegm(date_1.timetuple())
    ## time diff from midnight 00:00:00 : in secs
    df = df.withColumn("time_diff_mid", df.eventTs - F.lit(date_1_ts))
    # adjustment ( for data dump inaccuracies)
    # filetring out ping with hour index not in "1-86400", between includes both 
    df = df.filter(F.col("time_diff_mid").between(1,86400))
    # df = df.repartition(200) : keeping off for reducing shuffles
    ## fix index level , ex:  4 hours : 3600*4 secs
    index_sec = 3600 *  index_hrs_day
    #creating index col  : 1 to 6
    df = df.withColumn("index", F.ceil(df.time_diff_mid / F.lit(index_sec)))
    df = df.drop("time_diff_mid")
    return df



def summ_ifa_index_day(df):
    # df should contain cols:  "ifa", "index","eventTs", "geoHash7" : add check : print statement for mismatch
    ## summarisation on index level for an ifa:
    # note : count of eventTs as count of pings (assuming ifa + eventTs as key for staypoint output data)
    df_grp_ind = df.groupBy("ifa", "index").agg(F.count("eventTs").alias('pings_index') , F.countDistinct("geoHash7").alias('gh7s_index') )
    df = df.join(df_grp_ind, ["ifa", "index"], "inner").repartition(200)
    # unpersist : df_grp_ind 
    ## on day level for an ifa: 
    # note : dont use F.sum(gh7s_index) for gh7s_date 
    df_grp_ifa = df.groupBy("ifa").agg(F.countDistinct("index").alias('c_index'), F.countDistinct("geoHash7").alias('gh7s_date'), 
                            F.sum("pings_index").alias('pings_date'))
    df = df.drop("eventTs", "geoHash7").dropDuplicates()                
    df = df.join(df_grp_ifa, ["ifa"], "inner").repartition(200)
    # unpersist : df_grp_ifa
    return df

########## next to test : USA like con : dont take union using reduce, 
## dont create gh7s_index, gh7s_date' if not nes. needed
# can put if condition 
## Aply filter here itself : if not looking for iterations around c_index
# can put if condition 

#### saving cnt_index_cnt_ifa : as daily files
def fun_save_cnt_index_cnt_ifa( df, date_1 ,OUTPUT_DATA_PATH ): 
    cnt_index_cnt_ifa = df.groupBy("c_index").agg(F.countDistinct("ifa").alias('count_ifa'))
    cnt_index_cnt_ifa = cnt_index_cnt_ifa.withColumn("date",F.lit(date_1).cast("date"))
    cnt_of_cnt_path = OUTPUT_DATA_PATH + 'cnt_index_cnt_ifa/' + '{:04d}/{:02d}/{:02d}'.format(date_1.year,date_1.month,date_1.day)
    cnt_index_cnt_ifa.repartition(1).write.csv(cnt_of_cnt_path ,header = True, mode='overwrite')
    # unpersist : cnt_index_cnt_ifa

#### working with one day data
def pings_per_day(country, date_1,  BASE_PATH, index_hrs_day , save_cnt_index_cnt_ifa_day):
    print(date_1)
    # Adding country and index to the path
    OUTPUT_DATA_PATH = BASE_PATH +"%s/"%(country)  +"%s/"%(index_hrs_day)
    ## fun. calls:
    # sp data:
    data_pings_1 = get_one_day_sp_dataframe(date_1, country )
    # adding time index as new col
    data_pings_1 = add_index_eventTs(date_1, data_pings_1, index_hrs_day )
    # summarisation:
    data_pings_1 = summ_ifa_index_day(data_pings_1)
    # save cnt_index vs count_ifa as daily files:
    if save_cnt_index_cnt_ifa_day == True:
        fun_save_cnt_index_cnt_ifa(data_pings_1, date_1 , OUTPUT_DATA_PATH  )
    ## saving daily files (uncondtional)
    # add date as col:
    data_pings_1 = data_pings_1.withColumn("date",F.lit(date_1).cast("date"))
    counts_path = OUTPUT_DATA_PATH  + 'daily_counts/' + '{:04d}/{:02d}/{:02d}'.format(date_1.year,date_1.month,date_1.day)
    data_pings_1.write.parquet(counts_path, mode='overwrite')

########## next to test : USA like con : 
## see if adding date as col is redundant : reading from schema maybe
## see if function calls inc. reqd memory : update dfs on the go?? : less likely


#### looping for many days :  (date_1 -  num_of_days + 1) to date_1 : (including date_1)
def loop_pings_per_day(country, end_date, num_of_days, BASE_PATH,  index_hrs_day = 4, save_cnt_index_cnt_ifa_day = False):
    start_date = end_date - timedelta(num_of_days)
    daterange = pd.date_range(start=start_date, end=end_date, freq='D', normalize=True, closed='right')
    print(daterange)
    for dt in daterange:
        pings_per_day(country, dt, BASE_PATH , index_hrs_day, save_cnt_index_cnt_ifa_day )
        # print("done for the date" :dt)

### example:  usage 
# config. params:
country = "USA"
end_date = date(2019,12,24) 
num_of_days = 2
index_hrs_day = 4
BASE_PATH = 's3://near-datascience/arpan.shrivastava/attr_panel/v1_tests/final/'
save_cnt_index_cnt_ifa_day = False
## to do : Add today -  2 days (buffer) as date_1 : look into ping data updatate freq.


#### usage 
## for one day:
# pings_per_day(country, end_date, BASE_PATH , index_hrs_day, save_cnt_index_cnt_ifa_day)

## for (date_1 -  num_of_days) to date_1 
# can not use this for one day : empty list issue
loop_pings_per_day(country, end_date, num_of_days, BASE_PATH, index_hrs_day, save_cnt_index_cnt_ifa_day  )





















