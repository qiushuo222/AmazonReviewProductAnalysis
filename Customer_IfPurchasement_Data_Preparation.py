import sys

from pandas.core.indexes.api import all_indexes_same

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import json
from pyspark.sql import SparkSession, types
import pyspark.sql.functions as F
import os
from pyspark import SparkConf, SparkContext
import json
import datetime
import re
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
import numpy as np
'''
Market Basket Analysis
Input ==> Amazon product and review - Parquet
Output ==> Customer purchasement dataset - Json
'''

@F.udf(returnType=types.StringType())
def rm_amp(x):
    x = x.replace("amp;","")
    return x

@F.udf(returnType=types.IntegerType())
def sub_one(x):
    if x == 0:
        x = -1
    
    return x

def main(Amazon_Product_Review_Path, Output_Path):
    
    ## Get basic features prepared
    df_origin = spark.read.parquet(Amazon_Product_Review_Path) # df_origin = spark.read.parquet("/home/sqa13/home/bigdata/assignment/project/cmpt732/Amazon_Product_Review_Parquet_Part_00000")
    df_origin = df_origin.select("Product_Brand", "Product_Main_Category", F.col("Product_Rank").cast("float").alias("Product_Rank"), "Product_Price", F.col("Review_Vote").cast("float").alias("Review_Vote"), "Rate", "Product_Purchased")
    '''
    Data count = 1060218
    NULL_count:
    Reviewer_ID = 0
    Product_Asin = 0
    Product_Brand = 2395
    Product_Main_Category = 0
    Product_Rank = 5315
    Product_Price = 289743
    Review_Vote = 0
    Rate = 0
    Product_Purchased = 0
    '''

    df_origin = df_origin.filter((F.col("Product_Price").isNotNull()) & (F.col("Product_Brand").isNotNull()) & (F.col("Product_Rank").isNotNull()))
    df_origin = df_origin.select("Product_Brand", rm_amp(F.col("Product_Main_Category")).alias("Product_Main_Category"), "Product_Rank", "Product_Price", "Review_Vote", "Rate", sub_one(F.col("Product_Purchased")).alias("Product_Purchased"))
    df_distinct_brand = df_origin.dropDuplicates(["Product_Brand"]).select("Product_Brand")

    df_distinct_brand.registerTempTable("brand")
    df_distinct_brand_Encoder = spark.sql("SELECT Product_Brand, ROW_NUMBER() over(ORDER BY Product_Brand) Brand_Encoder FROM brand")

    df_distinct_Cat = df_origin.dropDuplicates(["Product_Main_Category"]).select("Product_Main_Category")
    df_distinct_Cat.registerTempTable("Cat")
    df_distinct_Cat_Encoder = spark.sql("SELECT Product_Main_Category, ROW_NUMBER() over(ORDER BY Product_Main_Category) Cat_Encoder FROM Cat")
    
    df_origin = df_origin.join(df_distinct_brand_Encoder, ["Product_Brand"], "left").repartition(12)
    df_origin = df_origin.join(df_distinct_Cat_Encoder, ["Product_Main_Category"], "left").repartition(12)
    
    df_result = df_origin.select("Product_Main_Category", F.col("Cat_Encoder").cast("float").alias("Cat_Encoder"), "Product_Brand", F.col("Brand_Encoder").cast("float").alias("Brand_Encoder"), "Product_Rank", "Product_Price", "Review_Vote", "Rate", "Product_Purchased")

    '''
    Data count = 767928
    '''
    '''
    +---------------------+-----------+--------------------+-------------+------------+-------------+-----------+----+-----------------+
    |Product_Main_Category|Cat_Encoder|       Product_Brand|Brand_Encoder|Product_Rank|Product_Price|Review_Vote|Rate|Product_Purchased|
    +---------------------+-----------+--------------------+-------------+------------+-------------+-----------+----+-----------------+
    |            Computers|       13.0|                Dell|       3961.0|     14269.0|         76.4|        0.0| 3.0|                1|
    |            Computers|       13.0|          Easy Style|       4606.0|     21056.0|        24.47|        0.0| 5.0|                1|
    |            Computers|       13.0|             Samsung|      13084.0|     10109.0|       399.95|        7.0| 1.0|               -1|
    |            Computers|       13.0|                 ZTC|      16827.0|       290.0|         0.37|        0.0| 4.0|                1|
    |            Computers|       13.0|             ProCase|      11853.0|        95.0|        15.99|        0.0| 5.0|                1|
    |            Computers|       13.0|SilverStone Techn...|      13485.0|      3854.0|         2.54|        0.0| 5.0|                1|
    |            Computers|       13.0|                ZAGG|      16792.0|       188.0|        54.99|        0.0| 3.0|                1|
    |            Computers|       13.0|           Transcend|      15198.0|       656.0|        13.99|        0.0| 5.0|                1|
    |            Computers|       13.0|           Medialink|       9657.0|      8466.0|         6.31|        3.0| 1.0|                1|
    |            Computers|       13.0|       Cooler Master|       3324.0|       383.0|        15.92|        0.0| 1.0|                1|
    |            Computers|       13.0|             Samsung|      13084.0|     10109.0|       399.95|        0.0| 5.0|                1|
    |            Computers|       13.0|      Bargains Depot|       1618.0|      2176.0|         6.99|        0.0| 5.0|               -1|
    |            Computers|       13.0|               Wacom|      16116.0|      6264.0|         4.07|        0.0| 3.0|                1|
    +---------------------+-----------+--------------------+-------------+------------+-------------+-----------+----+-----------------+
    '''

    df_origin.coalesce(1).write.json(Output_Path, mode="overwrite")

if __name__ == '__main__':
    sc = SparkContext()
    spark = SparkSession.builder.config("spark.driver.memory", "4g").appName("Amazon_Product_Reivew Market Basket Analysis").getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')

    Amazon_Product_Review_folder = sys.argv[1]
    Output_Path = sys.argv[2]
    Amazon_Product_Review_Path = Amazon_Product_Review_folder

    main(Amazon_Product_Review_Path, Output_Path)

