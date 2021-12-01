import re
import datetime
from pyspark import SparkConf, SparkContext
import os
from pyspark.sql import SparkSession, functions, types
import json
import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

if __name__ == '__main__':
    sc = SparkContext()
    spark = SparkSession.builder.config("spark.driver.memory", "2g").config(
        "spark.executor.memory", "6g").appName("Review ETL").getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')

    ffolder = os.path.split(os.path.abspath(__file__))[0]

    Amazon_Product_DF = spark.read.parquet(
        "/user/hza168/data/Amazon_Product_Review_Parquet/").cache()

    Amazon_Product_DF = Amazon_Product_DF.select(
        "*").where(functions.year(Amazon_Product_DF.Review_Post_Date) >= 2010)

# Category Seasonal Analysis

    seasonal_product = Amazon_Product_DF.withColumn(
        "Review_Post_Date", functions.quarter(Amazon_Product_DF.Review_Post_Date))

    seasonal_product_count = seasonal_product.select("*").where(seasonal_product.Product_Purchased == 1).groupBy(
        seasonal_product.Review_Post_Date, seasonal_product.Product_Main_Category).agg(functions.count(seasonal_product.Rate).alias("purchase_count"), functions.avg(seasonal_product.Rate).alias("avg_rate"))

    seasonal_product_count.select("*").where(seasonal_product_count["purchase_count"] > 100).orderBy(
        ["Product_Main_Category", "Review_Post_Date"]).show(n=3000)

    '''
    Amazon_Product_Review_Schema = types.StructType([
                                    types.StructField(
                                        "Product_Asin", types.StringType()),
                                    types.StructField(
                                        "Product_Brand", types.StringType()),
                                    types.StructField(
                                        "Product_Main_Category", types.StringType()),
                                    types.StructField(
                                        "Product_Rank", types.IntegerType()),
                                    types.StructField(
                                        "Product_Date", types.DateType()),
                                    types.StructField(
                                        "Product_Title", types.StringType()),
                                    types.StructField(
                                        "Product_Desc", types.StringType()),
                                    types.StructField(
                                        "Product_Shipping_Weight", types.StringType()),
                                    types.StructField(
                                        "Product_UPC", types.StringType()),
                                    types.StructField(
                                        "Product_Also_Buy", types.StringType()),
                                    types.StructField(
                                        "Product_Also_View", types.StringType()),
                                    types.StructField(
                                        "Product_Rank", types.StringType()),
                                    types.StructField(
                                        "Product_Style_Type", types.StringType()),
                                    types.StructField(
                                        "Product_Style_Value", types.StringType()),
                                    types.StructField(
                                        "Product_Price", types.FloatType()),
                                    types.StructField(
                                        "Reviewer_ID", types.StringType()),
                                    types.StructField(
                                        "Review_Vote", types.IntegerType()),
                                    types.StructField(
                                        "Reviewer_Name", types.StringType()),
                                    types.StructField(
                                        "Rate", types.FloatType()),
                                    types.StructField(
                                        "Review_Post_Date", types.DateType()),
                                    types.StructField(
                                        "Review_Title", types.StringType()),
                                    types.StructField(
                                        "Review_Content", types.StringType()),
                                    types.StructField(
                                        "Product_Purchased", types.IntegerType()),
                                    ])
    '''
