import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import json
from pyspark.sql import SparkSession, functions, types
import os

PATH_project = "D://Amazon metadata//meta_Gift_Cards.json"

 
spark = SparkSession.builder.config("spark.driver.memory", "4g").appName("Test ETL").getOrCreate()
assert spark.version >= '3.0' # make sure we have Spark 3.0+
spark.sparkContext.setLogLevel('WARN')
 
ffolder = os.path.split(os.path.abspath(__file__))[0]
DATA_PATH = os.path.join(ffolder, "testdata")





df = spark.read.json(DATA_PATH)

df.select("asin").show()
# with open(PATH_project) as f:
#     for i in f:
#         dic = json.loads(i)
#         df = pd.DataFrame(list(dic.items()))
#         df = df.T
#         df.columns = df.loc[0]
#         df = df.drop(0)
#         df_gift_card = df_gift_card.append(df, ignore_index = True)