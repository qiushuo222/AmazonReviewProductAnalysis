import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from pyspark.ml import PipelineModel
import matplotlib.pyplot as plt


# add more functions as necessary
@functions.udf(returnType=types.StringType())
def rm_amp(x):
    x = x.replace("amp;", "")
    return x

def main(model_file, inputs):
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



    df_origin = spark.read.parquet(inputs, schema=Amazon_Product_Review_Schema)

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
    
    # load the model
    model = PipelineModel.load(model_file)
    
    # use the model to make predictions
    predictions = model.transform(data_reformat)
    predictions.show()

    

if __name__ == '__main__':
    inputs = sys.argv[2]
    model_file = sys.argv[1]
    spark = SparkSession.builder.appName('category prediction').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(model_file, inputs)