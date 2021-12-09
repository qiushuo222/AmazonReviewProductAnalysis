import re
import datetime
from pyspark import SparkConf, SparkContext
import os
from pyspark.sql import SparkSession, functions, types
import json
import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+


def main(input, output):
    ffolder = os.path.split(os.path.abspath(__file__))[0]

    Amazon_Product_DF = spark.read.parquet(inputs)

    Amazon_Product_DF = Amazon_Product_DF.withColumn(
        'Product_Main_Category', functions.regexp_replace("Product_Main_Category", '&amp;', '&'))

    Amazon_Product_DF = Amazon_Product_DF.select(
        "Product_Asin",
        "Product_Main_Category",
        "Product_Price",
        "Reviewer_ID",
        "Review_Post_Date",
        "Rate",
        "Product_Purchased").where((functions.year(Amazon_Product_DF.Review_Post_Date) >= 2010) & (Amazon_Product_DF["Product_Purchased"] == 1)).repartition(64).cache()

    # Repurchase Analysis

    repurchase_count_df = Amazon_Product_DF.select(
        "*").where(Amazon_Product_DF["Product_Purchased"] == 1).groupBy("Product_Main_Category",
                                                                        "Product_Asin",
                                                                        "Reviewer_ID").agg(functions.count("Review_Post_Date").alias("purchase_count"),
                                                                                           functions.avg("Rate").alias("avg_rate")).select("*").where(functions.col("purchase_count") > 1).repartition(64).cache()

    repurchasers = repurchase_count_df.groupBy("Reviewer_ID").agg(
        functions.count(functions.col("avg_rate")).alias("repurchase_count")).orderBy(functions.col("repurchase_count").desc()).filter(functions.col("repurchase_count") >= 2).cache()

    repurchase_category = repurchase_count_df.groupBy("Product_Main_Category").agg(
        functions.sum(repurchase_count_df.purchase_count).alias("total_repurchase_count"))
    #   .groupBy(Amazon_Product_DF.Product_Main_Category)

    purchase_count_category_df = Amazon_Product_DF.groupBy(
        "Product_Main_Category").agg(functions.count(Amazon_Product_DF.Review_Post_Date).alias("total_sales")).select("Product_Main_Category", "total_sales")

    compare_repurchase = purchase_count_category_df.join(
        repurchase_category, purchase_count_category_df.Product_Main_Category == repurchase_category.Product_Main_Category, "inner").select(purchase_count_category_df.Product_Main_Category,
                                                                                                                                            purchase_count_category_df.total_sales,
                                                                                                                                            repurchase_category.total_repurchase_count)
    compare_repurchase = compare_repurchase.withColumn("repurchase_ratio", functions.col(
        "total_repurchase_count") / functions.col("total_sales") * 100)
    '''
    +---------------------+-----------+----------------------+-------------------+
    |Product_Main_Category|total_sales|total_repurchase_count|   repurchase_ratio|
    +---------------------+-----------+----------------------+-------------------+
    |            Computers|     127504|                   755| 0.5921382858576986|
    |      All Electronics|      87689|                  1985| 2.2636818757198736|
    |     GPS & Navigation|        507|                     6|  1.183431952662722|
    | Home Audio & Theater|      75618|                  1328|  1.756195614800709|
    |         Pet Supplies|       2034|                    12| 0.5899705014749262|
    |         Toys & Games|       3817|                   121|  3.170028818443804|
    |    Sports & Outdoors|       4840|                    54|  1.115702479338843|
    |              Grocery|        799|                    68|   8.51063829787234|
    |           Automotive|     111696|                   575| 0.5147901446784129|
    |          Amazon Home|     248145|                  5043|  2.032279513993834|
    | Industrial & Scie...|       4267|                   217|  5.085540192172487|
    | Health & Personal...|        581|                    24|  4.130808950086059|
    | Cell Phones & Acc...|       5433|                    10|0.18406037180195106|
    | Arts, Crafts & Se...|       2041|                    38| 1.8618324350808426|
    |       Amazon Devices|        718|                     5| 0.6963788300835655|
    |      Car Electronics|       6511|                    58| 0.8908001843034865|
    | Tools & Home Impr...|     123431|                  2930| 2.3737958859605772|
    |      Office Products|      49769|                  1444| 2.9014044887379695|
    |           Appliances|        533|                     6|  1.125703564727955|
    |       Camera & Photo|      57401|                   592|   1.03134091740562|
    |  Musical Instruments|      15171|                   281| 1.8522180475907983|
    | Portable Audio & ...|        950|                     6|  0.631578947368421|
    +---------------------+-----------+----------------------+-------------------+
    '''

    compare_repurchase_pd = compare_repurchase.toPandas()
    fig = compare_repurchase_pd.plot.bar(
        x='Product_Main_Category', y='repurchase_ratio', linewidth=50, rot=70, figsize=(20, 10), title="Percentage of Repurchases in Each Category").get_figure()
    fig.savefig(output + "repurchase_compare.png")


if __name__ == '__main__':
    sc = SparkContext()
    spark = SparkSession.builder.config("spark.driver.memory", "2g").config(
        "spark.executor.memory", "6g").appName("Review ETL").getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')

    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
