###### lib
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
spark = SparkSession.builder.appName('attr_panel_v1_aggn').getOrCreate()
sc = spark.sparkContext
sqlContext = SQLContext(sc)
# sc.setLogLevel("WARN")

                            

#### reading the daily files and aggn :

### cols to read :
##   TO DO : either fix the schemas in the build master data script:
# Or find a way to change the types while reading :
#currently is throws mismatch error.
def cols_to_read(read_index_level_info_day) :
    schema_index = StructType([ StructField("ifa",StringType(),True), StructField("date",DateType(),True),
                        StructField("index",LongType(),True),      StructField("pings_index",LongType(),True),
                        StructField("gh7s_index",LongType(),True), StructField("c_index",LongType(),True),
                        StructField("pings_date",LongType(),True), StructField("gh7s_date",LongType(),True) ])
    schema_day_level = StructType([ StructField("ifa",StringType(),True), StructField("date",DateType(),True),
                       StructField("c_index",LongType(),True),  StructField("pings_date",LongType(),True), 
                       StructField("gh7s_date",LongType(),True) ])
    # cols to read :
    if read_index_level_info_day == True:
        schema = schema_index
    else:
        schema = schema_day_level
    return schema



### logic to read from end_date and any given number of days:
## to do : add logic to check if exists for INPUT_DATA_PATH 
def get_aggregate_dataframe(end_date, num_of_days, INPUT_DATA_PATH , read_index_level_info_day = False, spark = spark):
    start_date = end_date - timedelta(num_of_days)
    daterange = pd.date_range(start=start_date, end=end_date, freq='D', normalize=True, closed='right')
    print(daterange)
    paths_list = []
    for dt in daterange:
        dt_str = dt.date().strftime("%Y/%m/%d")
        path_date = INPUT_DATA_PATH +dt_str+'/*'
        paths_list.append(path_date)
    print(paths_list)
    # schema:
    schema = cols_to_read(read_index_level_info_day)
    # read df
    df = spark.read.schema(schema).parquet(*paths_list)
    ## reducing the duplicate rows if applicable:
    if read_index_level_info_day == False:
        df = df.dropDuplicates()
    return df


### saving cnt_dates vs cnt_ifa :
def cnt_dates_cnt_ifa( df , OUTPUT_DATA_PATH ): 
    cnt_dates_cnt_ifa = df.groupBy("c_dates").agg(F.countDistinct("ifa").alias('count_ifa'))
    # write to s3 : small file 
    cnt_dates_cnt_ifa_path = OUTPUT_DATA_PATH + 'cnt_dates_cnt_ifa.csv' 
    cnt_dates_cnt_ifa.repartition(1).write.csv(cnt_dates_cnt_ifa_path, header = True, mode='overwrite')
    # unpersist : cnt_index_cnt_ifa


def apply_filters_c_index_c_dates(df, c_index_min_for_day, c_dates_min, OUTPUT_DATA_PATH , save_cnt_dates_cnt_ifa = True):
    ## filter on c_index first : 
    df = df.filter(df.c_index >= c_index_min_for_day )
    df = df.repartition(200)
    ## adding c_dates and total_pings for an ifa
    df_grp_ifa = df.groupBy("ifa").agg(F.countDistinct("date").alias('c_dates') , 
                                                        F.sum("pings_date").alias('total_pings'))
    df = df.join(df_grp_ifa, "ifa", "inner").repartition(200)
    # unpersist : df_grp_ifa
    # saving cnt_dates  vs cnt_ifa
    if save_cnt_dates_cnt_ifa == True:
        cnt_dates_cnt_ifa( df, OUTPUT_DATA_PATH )
        print("saving count of dates vs count of ifa disn")
    ## condition on c_dates:
    df = df.filter(df.c_dates >=  c_dates_min)
    ## writing the final data:
    final_path = OUTPUT_DATA_PATH  + 'final_data_ifa_date/' 
    df.write.parquet(final_path, mode ='overwrite')
    final_path_ifa = OUTPUT_DATA_PATH  + 'final_data_ifa/' 
    df.select("ifa", "c_dates", "total_pings").dropDuplicates().write.parquet(final_path_ifa, mode='overwrite')
    
    # return df




############## inputs :
country = 'AUS'
index_hrs_day = 4
end_date = date(2019,12,31)
num_of_days = 31
read_index_level_info_day = False # default false 
#used in reading the sutiable data
# false : day level data 5 cols key with ifa + date as key,true : day+index level data 8 cols key with ifa + date + index as key
c_index_min_for_day = 3 # range : 1 to 6 : 1 to 24/index_hrs_day
c_dates_min = 20 # range : 1 to num_of_days : default : num_of_days (everyday)
# note: num_of_days is  more : more is the memory reqd : aggn part 
save_cnt_dates_cnt_ifa = True # defualt true

### to do: upate for if condition:
# save_ifa_level_info_only = True # defualt False
# false : final data -- ifa + day level  , 7 cols key with ifa + date as key,  
# true : final data -- ifa level data 3 cols ifa, c_dates, total_pings with ifa as key

# BASE s3 location:
BASE_PATH = "s3://near-datascience/arpan.shrivastava/attr_panel/v1_tests/final/"
# from where to read:
INPUT_DATA_PATH  = BASE_PATH +"%s/"%(country)  +"%s/"%(index_hrs_day) + "daily_counts/"
# where to write:
OUTPUT_DATA_PATH  = BASE_PATH +"%s/"%(country)  +"%s/"%(index_hrs_day) + "aggn/" +"%s,"%(end_date) +"%s,"%(num_of_days ) +"%s,"%(c_index_min_for_day) +"%s/"%(c_dates_min )




###### calls
# read from master data:
daily_data_agg = get_aggregate_dataframe(end_date, num_of_days, INPUT_DATA_PATH, read_index_level_info_day)
print("daily files fetched and aggted")
# applying filter on cnt index and cnt of dates:
apply_filters_c_index_c_dates(daily_data_agg, c_index_min_for_day,  c_dates_min, OUTPUT_DATA_PATH ) 
# daily_data_agg = apply_filters_c_index_c_dates(daily_data_agg, c_index_min_for_day,  c_dates_min, OUTPUT_DATA_PATH ) 
# return df for further analysis
print("applied filters and saved the result data to s3 ")


















######## furthar analysis on basic selection: need to autmate : later


#### quantiles of : total pings 
# quantiles_to_return = [0.0, 0.0005, 0.001, 0.0025, 0.005, 0.01 ,0.03, 0.05, 0.1, 0.15, 0.2, 0.25, 0.5, 0.75, 0.9,  0.95, 1.0]
# q_score = SF(daily_data_agg_means).approxQuantile( "", quantiles_to_return , 0.0)

###### sample count of count disn : for total pings:
# from pyspark.sql.utils import AnalysisException
# from pyspark.sql import Row


# def has_column(df, col):
#     try:
#         df[col]
#         return True
#     except AnalysisException:
#         return False
# if has_column(df, "total_pings") == False:

# df = df.select("ifa" , "total_pings").dropDuplicates()
# # # daily_data_agg_no_zero_1d.repartition(1).write.csv("s3://near-datascience/arpan.shrivastava/attr_panel/v0_tests/1/daily_data_agg_no_zero_1d_sample_disn.csv" ,mode='overwrite')

# # ## count of pings vs count of ifa: should do in native py?? : small data 
# # cnt_of_cnt = daily_data_agg_no_zero_1d.groupBy("2019-11-10").agg(F.countDistinct("ifa").alias('count_ifa'))
# # w1 = Window.orderBy("2019-11-10")
# # cnt_of_cnt = cnt_of_cnt.withColumn("cumm_sum_ifa", F.sum('count_ifa').over(w1))
# # cnt_of_cnt.repartition(1).write.csv("s3://near-datascience/arpan.shrivastava/attr_panel/v0_tests/1/daily_data_agg_no_zero_1d_sample_disn_cnt_of_cnt.csv" ,header = True, mode='overwrite')














# # ### thr list to consider: 
# min  = int(q_score[1]) ## 0.05 %ile
# max  = int(q_score[7]) ## 5 %ile
# cut_off_list = pd.DataFrame({'cut':  range(min, max , 6 ) })


# ### write a fun for a thr t:
# ## update this : sort thr list,
# #  start from the max value,  filter out the > ,then chained condition checks : rathar than one by one 
# def count_pings_cut(t, df = daily_data_agg_means):
#     df = df.where( df.mean_pings_date_ifa >= t)
#     count_ifa = df.select("ifa").distinct().count()
#     return count_ifa
# cut_off_list['count_ifa'] = cut_off_list['cut'].apply(count_pings_cut)

# ## write to s3:
# cut_off_list_means_path = OUTPUT_DATA_BASE_PATH+ 'cut_off_list_means_vs_ifa.csv' 
# cut_off_list = spark.createDataFrame(cut_off_list)
# cut_off_list.repartition(1).write.csv(cut_off_list_means_path, mode='overwrite')

# ########### > k every day:
# k = 48
# daily_data_agg  = daily_data_agg.withColumn("grt_thn_k", F.when(daily_data_agg.pings_date > k , 1 ).otherwise(0))

# window1 = Window.partitionBy("ifa") 
# daily_data_agg  = daily_data_agg.withColumn("c_dates_grt_thn_k", F.sum('grt_thn_k').over(window1) ) 
# ### disn of c_dates_grt_thn_k vs #ifa:
# cnt_of_cnt_dates_k = daily_data_agg.groupBy("c_dates_grt_thn_k").agg(F.countDistinct("ifa").alias('count_ifa'))

# cnt_of_cnt_dates_k_path = OUTPUT_DATA_BASE_PATH + 'cnt_of_cnt_dates_k_ifa.csv' 
# cnt_of_cnt_dates_k.repartition(1).write.csv(cnt_of_cnt_dates_k_path, header = True, mode='overwrite')

# ## writing ifa + date : before appling condition:
# day_counts_path = OUTPUT_DATA_BASE_PATH+ 'day_counts_everyday_cnt_k/' 
# daily_data_agg.write.parquet(day_counts_path ,mode='overwrite')


# ### filtering for c_dates_grt_thn_k == 30
# daily_data_agg_panel = daily_data_agg.filter(daily_data_agg.c_dates_grt_thn_k == 30).drop("grt_thn_k", "c_dates_grt_thn_k") 
# # grt_thn_k = 1, c_dates_grt_thn_k" =30 for all rows

# # ## pivot 
# # daily_data_agg_panel = daily_data_agg_panel.groupBy("ifa").pivot("date").sum("pings_date","gh8s_date")
# daily_data_agg_panel = daily_data_agg_panel.groupBy("ifa").pivot("date").sum("pings_date")
# daily_data_agg_panel = daily_data_agg_panel.join(daily_data_agg_means, "ifa", "inner")


# ## writing ifa : pivot data : one row per ifa
# panel_output_path = OUTPUT_DATA_BASE_PATH+ 'panel_output_pivot/' 
# daily_data_agg_panel.write.parquet( panel_output_path, mode='overwrite')



# # ### notes:
# # ## long to wide 
# # ## 1 row per ifa
# # ## added information: zero daily count for IFAs ## i1 d1 0
# # ## costly operation : do only if necessary 
# # ## sum is dummy (count_pings is on day level already) : this could be done without daily aggn then : 1st stage output combine

 














