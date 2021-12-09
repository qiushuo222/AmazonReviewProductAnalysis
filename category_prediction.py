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

    # drop duplicate data
    data = data.distinct()
    data = data.cache()

    # Prediction on category
    data_reformat = data.withColumn("pt", functions.split(data["Product_Title"], " "))
    data_reformat = data_reformat.withColumn("pd", functions.split(data["Product_Desc"], " "))
    data_reformat = data_reformat.withColumn("pb", functions.split(data["Product_Brand"], " "))

    # data_reformat = data_reformat.select("pt", "pd", "Product_Main_Category")
    data_reformat = data_reformat.select("pt", "pd", "pb", "Product_Main_Category")
    data_reformat = data_reformat.cache()
    # data_reformat.show()
    
    train, validation, test = data_reformat.randomSplit([0.2, 0.2, 0.6])
    train = train.cache()
    validation = validation.cache()
    test = test.cache()

    cv_pt = CountVectorizer(inputCol="pt", outputCol="vectors_pt")
    cv_pd = CountVectorizer(inputCol="pd", outputCol="vectors_pd")
    cv_pb = CountVectorizer(inputCol="pb", outputCol="vectors_pb")
    labelIndexer = StringIndexer(inputCol="Product_Main_Category", outputCol="label", handleInvalid="keep")
    vecAssembler = VectorAssembler(inputCols=["vectors_pt", "vectors_pd", "vectors_pb"], outputCol="features", handleInvalid="keep")

    # Train a naive Bayes model.
    model = NaiveBayes()
    pipeline = Pipeline(stages=[cv_pt, cv_pd, cv_pb, labelIndexer, vecAssembler, model])
    model = pipeline.fit(train)

    predictions = model.transform(train)

    evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction")
    accuracy = evaluator.evaluate(predictions)
    print ("Model Accuracy: ", accuracy)

    predictions = model.transform(validation)

    evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction")
    accuracy = evaluator.evaluate(predictions)
    print ("Model Accuracy: ", accuracy)

    

if __name__ == '__main__':
    inputs = sys.argv[1]
    spark = SparkSession.builder.appName('category prediction').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs)