import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import json
from pyspark.sql import SparkSession, functions, types
import os
from pyspark import SparkConf, SparkContext
import json
import datetime
import re

'''
ETL for Amazon review data and join back to Amazon Product data
Input ==> Structed data of product information, semi-structed review dataset
Data cleaning, data transformation
Output ==> Amazon Product and Review information, structed data with below schema
Amazon_Product_Review_Schema = types.StructType([
                                types.StructField("Product_Asin", types.StringType()),
                                types.StructField("Product_Brand", types.StringType()),
                                types.StructField("Product_Main_Category", types.StringType()),
                                types.StructField("Product_Rank", types.IntegerType()),
                                types.StructField("Product_Date", types.DateType()),
                                types.StructField("Product_Title", types.StringType()),
                                types.StructField("Product_Desc", types.StringType()),
                                types.StructField("Product_Shipping_Weight", types.StringType()),
                                types.StructField("Product_UPC", types.StringType()),
                                types.StructField("Product_Also_Buy", types.StringType()),
                                types.StructField("Product_Also_View", types.StringType()),
                                types.StructField("Product_Rank", types.StringType()),
                                types.StructField("Product_Style_Type", types.StringType()),
                                types.StructField("Product_Style_Value", types.StringType()),
                                types.StructField("Product_Price", types.FloatType()),
                                types.StructField("Reviewer_ID", types.StringType()),
                                types.StructField("Review_Vote", types.IntegerType()),
                                types.StructField("Reviewer_Name", types.StringType()),
                                types.StructField("Rate", types.FloatType()),
                                types.StructField("Review_Post_Date", types.DateType()),
                                types.StructField("Review_Title", types.StringType()),
                                types.StructField("Review_Content", types.StringType()),
                                types.StructField("Product_Purchased", types.IntegerType()),    
                                ])

'''

def jsonload(review_record):
    temp = json.loads(review_record)
    reviewerID, asin, vote, reviewerName, overall, time_stamp, summary, reviewtext, style, verified = \
    (temp.get("reviewerID"), temp.get("asin"), temp.get("vote"), temp.get("reviewerName"), temp.get("overall"), temp.get("unixReviewTime"), temp.get("summary"), temp.get("reviewText"), temp.get("style"),temp.get("verified"))


    if isinstance(reviewerID, str):
        if reviewerID == "":
            reviewerID = None
    else:
        reviewerID = None

    if isinstance(asin, str):
        if asin == "":
            asin = None
    else:
        asin = None

    if isinstance(vote, str):
        if vote != "":
            vote = int(vote.replace(",", ""))
        else:
            vote = 0
    else:    
        vote = 0

    if isinstance(reviewerName, str):
        if reviewerName == "":
            reviewerName = None
    else:
        reviewerName = None
    
    if not isinstance(overall, float):
        overall = None

    if isinstance(time_stamp, int):
        time_stamp = datetime.datetime.fromtimestamp(time_stamp).strftime("%Y-%m-%d")
        time_stamp = datetime.datetime.strptime(time_stamp, "%Y-%m-%d")
    else:
        time_stamp = None

    # Details with Shipping Weight and UPC code
    if not isinstance(summary, str):
        summary = None

    if not isinstance(reviewtext, str):
        reviewtext = None

    if isinstance(style, dict):
        if style == {}:
            style = None
            style_type = None
            style_value = None
        else:
            for k, v in style.items():    
                style_type = k.replace(":", "")
                style_value = v
    else:
        style = None
        style_type = None
        style_value = None

    if isinstance(verified, bool):
        if verified == True:
            verified = 1
        else:
            verified = 0
    else:
        verified = None

    return (reviewerID, asin, vote, reviewerName, overall, time_stamp, summary, reviewtext, style_type, style_value, verified)

def main(Review_Path, Product_Path):
    Amazon_Review_RDD = sc.textFile(Review_Path)
    # rdd.saveAsTextFile("/home/sqa13/home/bigdata/assignment/project/cmpt732/testdata/data/", "org.apache.hadoop.io.compress.GzipCodec")
    Amazon_Review_RDD = Amazon_Review_RDD.map(jsonload)

    # 75,257,650 RDD
    Amazon_Review_Schema = types.StructType([
                            types.StructField("review_reviewerID", types.StringType()),
                            types.StructField("review_asin", types.StringType()),
                            types.StructField("review_vote", types.IntegerType()),
                            types.StructField("reviewer_Name", types.StringType()),
                            types.StructField("review_overall_score", types.FloatType()),
                            types.StructField("review_date", types.DateType()),
                            types.StructField("review_summary", types.StringType()),
                            types.StructField("review_text", types.StringType()),
                            types.StructField("review_product_style_type", types.StringType()),
                            types.StructField("review_product_style_value", types.StringType()),
                            types.StructField("review_verified", types.IntegerType())          
                            ])
    
    Amazon_Review_DF = Amazon_Review_RDD.toDF(schema = Amazon_Review_Schema)

    # 75,257,650 data points
    # Amazon_Review_DF.write.parquet(ffolder + "/Amazon_Review_Parquet", mode = "overwrite")
    # Clean data, 75,257,650 data point
    # Amazon_Review_DF = Amazon_Review_DF.filter((Amazon_Review_DF["review_reviewerID"].isNotNull())\
    #                                               & (Amazon_Review_DF["review_asin"].isNotNull() )\
    #                                               & (Amazon_Review_DF["review_overall_score"].isNotNull())
    #                                             )


    Amazon_Product_DF = spark.read.parquet(Product_Path)

    Amazon_Product_Review_DF = Amazon_Review_DF.join(Amazon_Product_DF, Amazon_Product_DF["asin"] == Amazon_Review_DF["review_asin"])

    # 5,301,185 data points
    Amazon_Product_Review_DF = Amazon_Product_Review_DF.select(\
                                                        functions.col("asin").alias("Product_Asin"),\
                                                        functions.col("brand").alias("Product_Brand"),\
                                                        functions.col("main_cat").alias("Product_Main_Category"),\
                                                        functions.col("rank").alias("Product_Rank"),\
                                                        functions.col("date").alias("Product_Date"),\
                                                        functions.col("title").alias("Product_Title"),\
                                                        functions.col("desc").alias("Product_Desc"),\
                                                        functions.col("shipping_weight").alias("Product_Shipping_Weight"),\
                                                        functions.col("UPC").alias("Product_UPC"),\
                                                        functions.col("also_buy").alias("Product_Also_Buy"),\
                                                        functions.col("also_view").alias("Product_Also_View"),\
                                                        functions.col("review_product_style_type").alias("Product_Style_Type"),\
                                                        functions.col("review_product_style_value").alias("Product_Style_Value"),\
                                                        functions.col("price").alias("Product_Price"),\
                                                        functions.col("review_reviewerID").alias("Reviewer_ID"),\
                                                        functions.col("review_vote").alias("Review_Vote"),\
                                                        functions.col("reviewer_Name").alias("Reviewer_Name"),\
                                                        functions.col("review_overall_score").alias("Rate"),\
                                                        functions.col("review_date").alias("Review_Post_Date"),\
                                                        functions.col("review_summary").alias("Review_Title"),\
                                                        functions.col("review_text").alias("Review_Content"),\
                                                        functions.col("review_verified").alias("Product_Purchased")
                                                        )


    Amazon_Product_Review_DF1 = Amazon_Product_Review_DF.orderBy(functions.asc("Product_Main_Category")).repartition(5)
    # Amazon_Product_Review_DF_Count = Amazon_Product_Review_DF1.agg(functions.countDistinct(functions.col("Product_Main_Category"))).collect()[0][0]
    # Amazon_Product_Review_DF1 = Amazon_Product_Review_DF1.repartitionByRange(Amazon_Product_Review_DF_Count, functions.col("Product_Main_Category"))

    Amazon_Product_Review_DF1.write.parquet(ffolder + "/Amazon_Product_Review_Parquet", mode = "append")
    Amazon_Product_Review_DF1.write.json(ffolder + "/Amazon_Product_Review_Json", mode = "append", compression = "gzip")

    '''
    Amazon_Product_Review_Schema = types.StructType([
                                    types.StructField("Product_Asin", types.StringType()),
                                    types.StructField("Product_Brand", types.StringType()),
                                    types.StructField("Product_Main_Category", types.StringType()),
                                    types.StructField("Product_Rank", types.IntegerType()),
                                    types.StructField("Product_Date", types.DateType()),
                                    types.StructField("Product_Title", types.StringType()),
                                    types.StructField("Product_Desc", types.StringType()),
                                    types.StructField("Product_Shipping_Weight", types.StringType()),
                                    types.StructField("Product_UPC", types.StringType()),
                                    types.StructField("Product_Also_Buy", types.StringType()),
                                    types.StructField("Product_Also_View", types.StringType()),
                                    types.StructField("Product_Rank", types.StringType()),
                                    types.StructField("Product_Style_Type", types.StringType()),
                                    types.StructField("Product_Style_Value", types.StringType()),
                                    types.StructField("Product_Price", types.FloatType()),
                                    types.StructField("Reviewer_ID", types.StringType()),
                                    types.StructField("Review_Vote", types.IntegerType()),
                                    types.StructField("Reviewer_Name", types.StringType()),
                                    types.StructField("Rate", types.FloatType()),
                                    types.StructField("Review_Post_Date", types.DateType()),
                                    types.StructField("Review_Title", types.StringType()),
                                    types.StructField("Review_Content", types.StringType()),
                                    types.StructField("Product_Purchased", types.IntegerType()),    
                                    ])
    '''
if __name__ == '__main__':
    sc = SparkContext()
    spark = SparkSession.builder.config("spark.driver.memory", "2g").config("spark.executor.memory", "6g").appName("Review ETL").getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')

    ffolder = os.path.split(os.path.abspath(__file__))[0]
    Review_folder = sys.argv[1]
    Product_folder = sys.argv[2]
    Review_Path = os.path.join(ffolder, Review_folder)
    Product_Path = os.path.join(ffolder, Product_folder)
    main(Review_Path, Product_Path)

