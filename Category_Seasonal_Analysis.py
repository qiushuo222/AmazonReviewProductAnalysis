import matplotlib.pyplot as plt
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, SQLTransformer
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
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

    Amazon_Product_DF = Amazon_Product_DF.select(
        "*").where((functions.year(Amazon_Product_DF.Review_Post_Date) >= 2010) & (Amazon_Product_DF.Product_Price.isNotNull())).cache()

    # Category Seasonal Analysis

    seasonal_product = Amazon_Product_DF.withColumn(
        "Quarter", functions.quarter(Amazon_Product_DF.Review_Post_Date))

    seasonal_product = seasonal_product.withColumn(
        "Year", functions.year(seasonal_product.Review_Post_Date))

    seasonal_product_count = seasonal_product.select("*").where(seasonal_product.Product_Purchased == 1).groupBy(
        seasonal_product.Year, seasonal_product.Quarter, seasonal_product.Product_Main_Category).agg(functions.count(seasonal_product.Rate).alias("purchase_count"),
                                                                                                     functions.avg(seasonal_product.Rate).alias(
                                                                                                         "avg_rate"),
                                                                                                     functions.avg(seasonal_product.Product_Price).alias("avg_price")).repartition(64).cache()

    # seasonal_product_count.select("*").where(seasonal_product_count["purchase_count"] > 100).orderBy(
    #     ["Year", "Quarter", "Product_Main_Category"]).show(n=5)

    '''
    +----+-------+---------------------+--------------+------------------+------------------+
    |Year|Quarter|Product_Main_Category|purchase_count|          avg_rate|         avg_price|
    +----+-------+---------------------+--------------+------------------+------------------+
    |2010|      1|      All Electronics|           366| 4.224043715846994|17.442240472048358|
    |2010|      1|          Amazon Home|           425| 4.383529411764706|26.899576466504264|
    |2010|      1|       Camera & Photo|           183| 4.229508196721311|52.502294830285784|
    '''

    seasonal_avg = seasonal_product_count.groupBy(seasonal_product_count.Quarter, seasonal_product_count.Product_Main_Category).agg(
        (functions.avg(seasonal_product_count.purchase_count)).alias("avg_sales"))
    # seasonal_avg.show(n=5)

    # Plot pie chart using pandas
    seasonal_avg_pd = seasonal_avg.toPandas()
    # plot = seasonal_product_count_pd.plot.pie(y='mass', figsize=(5, 5))
    # print("============================================================================================")
    # print(seasonal_avg_pd.head(10))
    seasonal_avg_pd = seasonal_avg_pd.set_index(
        ['Quarter', 'Product_Main_Category']).avg_sales.unstack()
    '''
            Product_Main_Category  All Beauty  All Electronics  ...  Toys & Games  Video Games
            Quarter                                             ...
            1                        6.833333      1857.000000  ...    118.222222     9.250000
            2                        5.000000      1478.000000  ...     75.111111     8.857143
            3                        6.000000      1489.555556  ...     69.666667     8.285714
            4                        7.833333      1609.111111  ...    103.750000    10.000000
    '''
    fig, axes = plt.subplots(2, 2, figsize=(20, 12))

    for i, (idx, row) in enumerate(seasonal_avg_pd.iterrows()):
        ax = axes[i // 2, i % 2]
        row = row[row.gt(row.sum() * .01)]
        ax.pie(row, labels=row.index, startangle=30, autopct='%1.1f%%')
        ax.set_title(idx)
    fig.subplots_adjust(wspace=.2)
    fig.savefig(output + "Seasonal_Analysis_Pie_Chart.png")

    # Predict base on year, season, price, avg_price, category

    iter = 100
    maxDepth = 5
    model_output_folder = "project_output"

    train, validation = seasonal_product_count.randomSplit([0.75, 0.25])
    train = train.cache()
    validation = validation.cache()
    category_indexer = StringIndexer(
        inputCol="Product_Main_Category", outputCol="category_index")
    feature_assembler = VectorAssembler(
        inputCols=['Year',
                   'Quarter',
                   'category_index',
                   'purchase_count',
                   "avg_rate",
                   "avg_price"],
        outputCol='features')
    regressor = GBTRegressor(
        featuresCol="features", labelCol="purchase_count", maxIter=iter, maxDepth=maxDepth, maxBins=33)
    sales_count_pipeline = Pipeline(
        stages=[category_indexer, feature_assembler, regressor])

    # ==============================================================================Pass dataset into pipeline
    sales_count_model = sales_count_pipeline.fit(train)

    # ==============================================================================Save the model
    sales_count_model.write().overwrite().save(
        model_output_folder + "Category_Prediction_Model")

    prediction_dataset = sales_count_model.transform(train)
    prediction_dataset.show(n=2)

    r2_evaluator = RegressionEvaluator(
        predictionCol='prediction', labelCol='purchase_count',
        metricName='r2')
    r2 = r2_evaluator.evaluate(prediction_dataset)
    print("Train score: ", r2)

    prediction_dataset = sales_count_model.transform(validation)
    prediction_dataset.show(n=2)

    r2_evaluator = RegressionEvaluator(
        predictionCol='prediction', labelCol='purchase_count',
        metricName='r2')
    r2 = r2_evaluator.evaluate(prediction_dataset)
    print("Validation score: ", r2)

    '''
    +----+-------+---------------------+--------------+-----------------+------------------+--------------+--------------------+------------------+
    |Year|Quarter|Product_Main_Category|purchase_count|         avg_rate|         avg_price|category_index|            features|        prediction|
    +----+-------+---------------------+--------------+-----------------+------------------+--------------+--------------------+------------------+
    |2010|      2| Sports &amp; Outd...|             2|              4.5|               4.5|          22.0|[2010.0,2.0,22.0,...|0.9815701116222529|
    |2016|      3| Tools &amp; Home ...|            89|3.808988764044944|29.211910251821024|           5.0|[2016.0,3.0,5.0,8...| 87.95719448249824|
    +----+-------+---------------------+--------------+-----------------+------------------+--------------+--------------------+------------------+
    only showing top 2 rows

    Train score:  0.9999767535289817
    +----+-------+---------------------+--------------+----------------+------------------+--------------+--------------------+------------------+
    |Year|Quarter|Product_Main_Category|purchase_count|        avg_rate|         avg_price|category_index|            features|        prediction|
    +----+-------+---------------------+--------------+----------------+------------------+--------------+--------------------+------------------+
    |2018|      2|            Computers|          3001|4.34921692769077|15.843941950194242|           2.0|[2018.0,2.0,2.0,3...| 3864.659469788557|
    |2013|      4|       Camera & Photo|          1158|4.34887737478411|47.118980810123404|          12.0|[2013.0,4.0,12.0,...|1177.7434270251501|
    +----+-------+---------------------+--------------+----------------+------------------+--------------+--------------------+------------------+
    only showing top 2 rows

    Validation score:  0.9730884249938129
    '''


if __name__ == '__main__':
    sc = SparkContext()
    spark = SparkSession.builder.config("spark.driver.memory", "2g").config(
        "spark.executor.memory", "6g").appName("Review ETL").getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')

    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)

#     ffolder = os.path.split(os.path.abspath(__file__))[0]

#     Amazon_Product_DF = spark.read.parquet(
#         "/user/hza168/data/Amazon_Product_Review_Parquet/")

#     Amazon_Product_DF = Amazon_Product_DF.select(
#         "*").where((functions.year(Amazon_Product_DF.Review_Post_Date) >= 2010) & (Amazon_Product_DF.Product_Price.isNotNull())).cache()

# # Category Seasonal Analysis

#     seasonal_product = Amazon_Product_DF.withColumn(
#         "Quarter", functions.quarter(Amazon_Product_DF.Review_Post_Date))

#     seasonal_product = seasonal_product.withColumn(
#         "Year", functions.year(seasonal_product.Review_Post_Date))

#     seasonal_product_count = seasonal_product.select("*").where(seasonal_product.Product_Purchased == 1).groupBy(
#         seasonal_product.Year, seasonal_product.Quarter, seasonal_product.Product_Main_Category).agg(functions.count(seasonal_product.Rate).alias("purchase_count"),
#                                                                                                      functions.avg(seasonal_product.Rate).alias(
#                                                                                                          "avg_rate"),
#                                                                                                      functions.avg(seasonal_product.Product_Price).alias("avg_price")).repartition(64).cache()

#     seasonal_product_count.select("*").where(seasonal_product_count["purchase_count"] > 100).orderBy(
#         ["Year", "Quarter", "Product_Main_Category"]).show(n=5)

#     '''
#     +----+-------+---------------------+--------------+------------------+------------------+
#     |Year|Quarter|Product_Main_Category|purchase_count|          avg_rate|         avg_price|
#     +----+-------+---------------------+--------------+------------------+------------------+
#     |2010|      1|      All Electronics|           366| 4.224043715846994|17.442240472048358|
#     |2010|      1|          Amazon Home|           425| 4.383529411764706|26.899576466504264|
#     |2010|      1|       Camera & Photo|           183| 4.229508196721311|52.502294830285784|
#     '''

#     seasonal_avg = seasonal_product_count.groupBy(seasonal_product_count.Quarter, seasonal_product_count.Product_Main_Category).agg(
#         (functions.avg(seasonal_product_count.purchase_count)).alias("avg_sales"))
#     # seasonal_avg.show(n=5)


# # Plot pie chart using pandas
#     seasonal_avg_pd = seasonal_avg.toPandas()
#     # plot = seasonal_product_count_pd.plot.pie(y='mass', figsize=(5, 5))
#     print("============================================================================================")
#     print(seasonal_avg_pd.head(10))
#     seasonal_avg_pd = seasonal_avg_pd.set_index(
#         ['Quarter', 'Product_Main_Category']).avg_sales.unstack()
#     '''
#             Product_Main_Category  All Beauty  All Electronics  ...  Toys & Games  Video Games
#             Quarter                                             ...
#             1                        6.833333      1857.000000  ...    118.222222     9.250000
#             2                        5.000000      1478.000000  ...     75.111111     8.857143
#             3                        6.000000      1489.555556  ...     69.666667     8.285714
#             4                        7.833333      1609.111111  ...    103.750000    10.000000
#     '''
#     fig, axes = plt.subplots(2, 2, figsize=(20, 12))

#     for i, (idx, row) in enumerate(seasonal_avg_pd.iterrows()):
#         ax = axes[i // 2, i % 2]
#         row = row[row.gt(row.sum() * .01)]
#         ax.pie(row, labels=row.index, startangle=30, autopct='%1.1f%%')
#         ax.set_title(idx)
#     fig.subplots_adjust(wspace=.2)
#     fig.savefig("pie_chart.png")

# # Predict base on year, season, price, avg_price, category

#     iter = 100
#     maxDepth = 5
#     model_output_folder = "project_output"

#     train, validation = seasonal_product_count.randomSplit([0.75, 0.25])
#     train = train.cache()
#     validation = validation.cache()
#     category_indexer = StringIndexer(
#         inputCol="Product_Main_Category", outputCol="category_index")
#     feature_assembler = VectorAssembler(
#         inputCols=['Year',
#                    'Quarter',
#                    'category_index',
#                    'purchase_count',
#                    "avg_rate",
#                    "avg_price"],
#         outputCol='features')
#     regressor = GBTRegressor(
#         featuresCol="features", labelCol="purchase_count", maxIter=iter, maxDepth=maxDepth, maxBins=33)
#     sales_count_pipeline = Pipeline(
#         stages=[category_indexer, feature_assembler, regressor])

#     # ==============================================================================Pass dataset into pipeline
#     sales_count_model = sales_count_pipeline.fit(train)

#     # ==============================================================================Save the model
#     sales_count_model.write().overwrite().save(model_output_folder)

#     prediction_dataset = sales_count_model.transform(train)
#     prediction_dataset.show(n=2)

#     r2_evaluator = RegressionEvaluator(
#         predictionCol='prediction', labelCol='purchase_count',
#         metricName='r2')
#     r2 = r2_evaluator.evaluate(prediction_dataset)
#     print("Train score: ", r2)

#     prediction_dataset = sales_count_model.transform(validation)
#     prediction_dataset.show(n=2)

#     r2_evaluator = RegressionEvaluator(
#         predictionCol='prediction', labelCol='purchase_count',
#         metricName='r2')
#     r2 = r2_evaluator.evaluate(prediction_dataset)
#     print("Validation score: ", r2)

#     '''
#     +----+-------+---------------------+--------------+-----------------+------------------+--------------+--------------------+------------------+
#     |Year|Quarter|Product_Main_Category|purchase_count|         avg_rate|         avg_price|category_index|            features|        prediction|
#     +----+-------+---------------------+--------------+-----------------+------------------+--------------+--------------------+------------------+
#     |2010|      2| Sports &amp; Outd...|             2|              4.5|               4.5|          22.0|[2010.0,2.0,22.0,...|0.9815701116222529|
#     |2016|      3| Tools &amp; Home ...|            89|3.808988764044944|29.211910251821024|           5.0|[2016.0,3.0,5.0,8...| 87.95719448249824|
#     +----+-------+---------------------+--------------+-----------------+------------------+--------------+--------------------+------------------+
#     only showing top 2 rows

#     Train score:  0.9999767535289817
#     +----+-------+---------------------+--------------+----------------+------------------+--------------+--------------------+------------------+
#     |Year|Quarter|Product_Main_Category|purchase_count|        avg_rate|         avg_price|category_index|            features|        prediction|
#     +----+-------+---------------------+--------------+----------------+------------------+--------------+--------------------+------------------+
#     |2018|      2|            Computers|          3001|4.34921692769077|15.843941950194242|           2.0|[2018.0,2.0,2.0,3...| 3864.659469788557|
#     |2013|      4|       Camera & Photo|          1158|4.34887737478411|47.118980810123404|          12.0|[2013.0,4.0,12.0,...|1177.7434270251501|
#     +----+-------+---------------------+--------------+----------------+------------------+--------------+--------------------+------------------+
#     only showing top 2 rows

#     Validation score:  0.9730884249938129
#     '''
