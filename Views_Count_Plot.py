import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, SQLTransformer, CountVectorizer
from pyspark.ml.feature import StringIndexer
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.classification import NaiveBayes, NaiveBayesModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
import matplotlib.pyplot as plt


# add more functions as necessary
@functions.udf(returnType=types.StringType())
def rm_amp(x):
    x = x.replace("amp;", "")
    return x

def main():
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



    df_origin = spark.read.parquet("amazon", schema=Amazon_Product_Review_Schema)

    data = df_origin.withColumn("Product_Main_Category_clean", rm_amp(df_origin["Product_Main_Category"]))
    data = data.drop(df_origin["Product_Main_Category"]).withColumnRenamed("Product_Main_Category_clean", "Product_Main_Category")

    data = data.filter("Product_Title is not null")
    data = data.filter("Product_Desc is not null")
    data = data.filter("Product_Brand is not null")
    data = data.select("Product_Title", "Product_Desc", "Product_Brand", "Product_Main_Category")

    data = data.cache()

    # figure plotting 
    data_count = data.groupBy("Product_Main_Category").count()

    data_count = data_count.toPandas()
    data_count = data_count.set_index('Product_Main_Category').sort_values(by = "count", ascending = False)

    plot = data_count.plot(kind='pie', y = "count", figsize=(20,10), autopct='%1.1f%%')
    plt.legend(loc="center left", bbox_to_anchor=(1.2, 0.5))
    plt.title("Popularity of Views Group By Main Categories")
    plt.savefig("Popularity of views pie")

    plot = data_count.plot(kind='bar', y = "count", figsize=(20,10))
    plt.subplots_adjust(bottom=0.3)
    plt.title("Amount of Views Group By Main Categories")
    plt.savefig("Amount of views bar")


    # drop duplicate data
    data = data.distinct()
    data = data.cache()

    # figure plotting 
    data_count = data.groupBy("Product_Main_Category").count()

    data_count = data_count.toPandas()
    data_count = data_count.set_index('Product_Main_Category').sort_values(by = "count", ascending = False)

    plot = data_count.plot(kind='pie', y = "count", figsize=(20,10), autopct='%1.1f%%')
    plt.legend(loc="center left", bbox_to_anchor=(1.2, 0.5))
    plt.title("Popularity of products with views group by Main Categories")
    plt.savefig("Popularity of products pie")

    plot = data_count.plot(kind='bar', y = "count", figsize=(20,10))
    plt.subplots_adjust(bottom=0.3)
    plt.title("Amount of products with views Group By Main Categories")
    plt.savefig("Amount of products bar")


if __name__ == '__main__':
    inputs = sys.argv[1]
    spark = SparkSession.builder.appName('views count plot').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs)