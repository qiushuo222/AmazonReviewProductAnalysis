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
asin string reserve
brand string reserve
main_cat string reserve

date string, change to datetime
title, string reserve
description list, change to string
details dict get Shipping Weight and UPC code to columns
also_buy list, change to string
also_view list, change to string
rank string, extract rank number and make it long
feature list??
price string, extract number and make it long
similar_item string

tech1 drop
tech2 drop
fit drop
imageURL drop
imageURLHighRes drop
category drop

aaa
'''

def jsonload(product_record):
    temp = json.loads(product_record)
    asin, brand, main_cat, date, title, desc, details, also_buy, also_view, rank, feature, price = \
    (temp.get("asin"), temp.get("brand"), temp.get("main_cat"), temp.get("date"), temp.get("title"), temp.get("description"), temp.get("details"), temp.get("also_buy"), temp.get("also_view"), temp.get("rank"), temp.get("feature"), temp.get("price"))#, temp.get("similar_item"))

    if isinstance(asin, str):
        if asin == "":
            asin = None
    else:
        asin = None

    if isinstance(brand, str):
        if brand == "":
            brand = None
    else:
        brand = None

    if isinstance(main_cat, str):
        if main_cat == "":
            main_cat = None
    else:
        main_cat = None
    
    # Modify date type
    if isinstance(date, str):
        if date == "":
            date = None
        else:
            date_re = re.compile(r'^([a-zA-Z]+\s\d,\s\d+)$')    
            date_match = date_re.match(date)
            if date_match != None:
                date = datetime.datetime.strptime(date, '%B %d, %Y') # December 2, 2015
            else:
                date = None
    else:
        date = None

    # Combine description to one string
    if isinstance(desc, list):
        if desc == []:
            desc = None
        else:
            desc = "||".join(desc)
    else:
        desc = None

    # Details with Shipping Weight and UPC code
    if isinstance(details, dict):
        if details == {}:
            details = None
            shipping_weight = None
            UPC = None
        else:
            shipping_weight = details.get("Shipping Weight")
            UPC = details.get("UPC")
    else:
        details = None
        shipping_weight = None
        UPC = None

    if isinstance(also_buy, list):
        if also_buy == []:
            also_buy = None
        else:
            also_buy = "||".join(also_buy).replace(" ", "")
    else:
        also_buy = None

    if isinstance(also_view, list):
        if also_view == []:
            also_view = None
        else:
            also_view = "||".join(also_view).replace(" ", "")
    else:
        also_view = None

    # Rank has two types: list or string
    if isinstance(rank, list):
        if rank == []:
            rank = None
        else:
            rank_re = re.compile(r'\D+([,0-9]+)')
            rank = rank_re.match(",".join(rank))
            if rank != None:
                rank = rank.group(1)
                rank = rank.replace(",", "")
                if rank != "":
                    rank = int(rank)
                else:
                    rank = None
    elif isinstance(rank, str):
        if rank == "":
            rank = None
        else:
            rank_re = re.compile(r'\D+([,0-9]+)')
            rank = rank_re.match(rank)
            if rank != None:
                rank = rank.group(1)
                rank = rank.replace(",", "")
                if rank != "":
                    rank = int(rank)
                else:
                    rank = None
    

    if isinstance(feature, list):
        if feature == []:
            feature = None
        else:
            feature = "||".join(feature)
    else:
        feature = None

    if isinstance(price, str):
        if price == "":
            price = None
        else:
            price_re = re.compile("\D+([\d]+.[\d]+)")
            price = price_re.match(price)
            if price != None:
                price = price.group(1).replace(",", "")
                if price != "":
                    price = float(price)
                else:
                    price = None
    else:
        price = None

    # if isinstance(similar_item, str):
    #     if similar_item == "":
    #         similar_item = None
    # else:
    #     similar_item = None

    return (asin, brand, main_cat, date, title, desc, shipping_weight, UPC, also_buy, also_view, rank, feature, price)#, similar_item)

def main(Product_Path):
    Amazon_Product_RDD = sc.textFile(Product_Path)
    # rdd.saveAsTextFile("/home/sqa13/home/bigdata/assignment/project/cmpt732/testdata/data/", "org.apache.hadoop.io.compress.GzipCodec")
    Amazon_Product_RDD = Amazon_Product_RDD.map(jsonload)

    Amazon_Product_Schema = types.StructType([
                            types.StructField("asin", types.StringType()),
                            types.StructField("brand", types.StringType()),
                            types.StructField("main_cat", types.StringType()),
                            types.StructField("date", types.DateType()),
                            types.StructField("title", types.StringType()),
                            types.StructField("desc", types.StringType()),
                            types.StructField("shipping_weight", types.StringType()),
                            types.StructField("UPC", types.StringType()),
                            types.StructField("also_buy", types.StringType()),
                            types.StructField("also_view", types.StringType()),
                            types.StructField("rank", types.IntegerType()),
                            types.StructField("feature", types.StringType()),
                            types.StructField("price", types.FloatType())
                            #types.StructField("simliar_item", types.StringType())               
                            ])
    
    Amazon_Product_DF = Amazon_Product_RDD.toDF(schema = Amazon_Product_Schema)

    # Clean data
    Amazon_Product_DF = Amazon_Product_DF.filter((~Amazon_Product_DF["main_cat"].startswith("<img"))\
                                                  & (Amazon_Product_DF["date"] != datetime.datetime.strptime("1973-09-04", '%Y-%m-%d'))\
                                                  & (Amazon_Product_DF["main_cat"].isNotNull())
                                                  #& (Amazon_Product_DF["date"].isNotNull())\
                                                  #& (Amazon_Product_DF["title"].isNotNull())\
                                                  #& (Amazon_Product_DF["desc"].isNotNull())\
                                                  #& (Amazon_Product_DF["also_buy"].isNotNull())\
                                                  #& (Amazon_Product_DF["also_view"].isNotNull())\
                                                  #& (Amazon_Product_DF["rank"].isNotNull())\
                                                  #& (Amazon_Product_DF["feature"].isNotNull())\
                                                  #& (Amazon_Product_DF["price"].isNotNull())    
                                                )
    # 955411 data point
    Amazon_Product_DF.write.parquet(ffolder + "/Amazon_Product_Parquet", mode = "overwrite")

if __name__ == '__main__':
    sc = SparkContext()
    spark = SparkSession.builder.config("spark.driver.memory", "4g").appName("Product ETL").getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')

    ffolder = os.path.split(os.path.abspath(__file__))[0]
    Product_folder = sys.argv[1]
    Product_Path = os.path.join(ffolder, Product_folder)
    main(Product_Path)

