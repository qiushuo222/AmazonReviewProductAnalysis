# 1. Running Envrionment
Python 3.5+, Spark 3.0+ with hadoop 3.2, Linux system
<br>
<br>
# 2. Project Code repository

https://csil-git1.cs.surrey.sfu.ca/hza168/cmpt732/-/tree/master

>\>git clone git@csil-git1.cs.surrey.sfu.ca:hza168/cmpt732.git<br>
>\>cd cmpt732<br>
>\>mkdir data<br>
>\>pwd<br>
~/home/bigdata/assignment/project/cmpt732

<br>
<br>

# 3. Data Preparation
This preparation of data is going to be a huge workload for Spark and memory consuming, so we have prepared small data set for instructors and TAs for testing purpose of other applications, they are available in testdata folder if you don't want to perform below steps.
<br>
<br>
## 1). Data Source
Download Amazon product metadata (11.5GB) and 5-core Review data (13.6GB) from below link:

>https://nijianmo.github.io/amazon/index.html#complete-data

Put metadata and 5-core Review data into two folders: Amazon_Product_Ori and Amazon_5_Core_Review_Ori
<br>
<br>
## 2). Pre-process for Spark

1. Put two downloaded folder in local file system or HDFS, i.e. in local file system

>Path = "~/home/bigdata/assignment/project/cmpt732/data/" <br>
>Product_data_path = Path + "Amazon_Product_Ori"<br>
>Review_data_path = Path + "Amazon_5_Core_Review_Ori"

<br>

2. Go into pyspark shell (with driver memory 8g if running standalone spark) and input below command:
```
df = spark.read.json(Product_data_path)
df = df.repartition(72)
df.write.json(Path + "Amazon_Product")
df1 = spark.read.json(Review_data_path)
df1 = df1.repartition(72)
df1.write.json(Path + "Amazon_5_Core_Review")
```
We need repartition these uneven spaced source data to better support Spark applications.<br>
After this step, we got below two folders for Spark:
> "~/home/bigdata/assignment/project/cmpt732/data/Amazon_Product" <br>
>"~/home/bigdata/assignment/project/cmpt732/data/Amazon_5_Core_Review"<br>

<br>

3. Divide 5 core review data into 12 batches by input below commands:
> \>cd ~/home/bigdata/assignment/project/cmpt732/data/Amazon_5_Core_Review<br>
> \>mkdir {1..12}_batch<br>
> \>mv part-0000[0-5].gz ./1_batch<br>
> \>mv part-0000[6-9].gz ./2_batch<br>
> \>mv part-0001[0-1].gz ./2_batch<br>
> \>mv part-0001[2-7].gz ./3_batch<br>
> \>mv part-0001[8-9].gz ./4_batch<br>
> \>mv part-0002[0-3].gz ./4_batch<br>
> \>mv part-0002[4-9].gz ./5_batch<br>
> \>mv part-0003[0-5].gz ./6_batch<br>
> \>mv part-0003[6-9].gz ./7_batch<br>
> \>mv part-0004[0-1].gz ./7_batch<br>
> \>mv part-0004[2-7].gz ./8_batch<br>
> \>mv part-0004[8-9].gz ./9_batch<br>
> \>mv part-0005[0-3].gz ./9_batch<br>
> \>mv part-0005[4-9].gz ./10_batch<br>
> \>mv part-0006[0-5].gz ./11_batch<br>
> \>mv part-0006[6-9].gz ./12_batch<br>
> \>mv part-0007[0-1].gz ./12_batch<br>

Due to review data is too huge and during ETL we need join product and review data, we have to deal with part of review data firstly one by one.
<br>
<br>

## 3). Basic ETL
1. Cleaning, Transformation, Load for Amazon product data (Consider increase driver or executor memory when execute spark-submit)
> \>cd ~/home/bigdata/assignment/project/cmpt732/<br>
> \>spark-submit Amazon_Product_ETL.py ./data/Amazon_Product


After this application done, in project folder it will generate a folder named "Amazon_Product_Parquet", and 72 parquet files within it.
<br>
<br>

2. Cleaning, Transformation, Load for Amazon review data, plus combine product and review information together<br>

> \>cd ~/home/bigdata/assignment/project/cmpt732/<br>
> \>spark-submit Amazone_Review_ETL.py ./data/Amazon_5_Core_Review/1_batch Amazon_Product<br>
> \>spark-submit Amazone_Review_ETL.py ./data/Amazon_5_Core_Review/2_batch Amazon_Product<br>
> \>spark-submit Amazone_Review_ETL.py ./data/Amazon_5_Core_Review/3_batch Amazon_Product<br>
> \>spark-submit Amazone_Review_ETL.py ./data/Amazon_5_Core_Review/4_batch Amazon_Product<br>
> \>spark-submit Amazone_Review_ETL.py ./data/Amazon_5_Core_Review/5_batch Amazon_Product<br>
> \>spark-submit Amazone_Review_ETL.py ./data/Amazon_5_Core_Review/6_batch Amazon_Product<br>
> \>spark-submit Amazone_Review_ETL.py ./data/Amazon_5_Core_Review/7_batch Amazon_Product<br>
> \>spark-submit Amazone_Review_ETL.py ./data/Amazon_5_Core_Review/8_batch Amazon_Product<br>
> \>spark-submit Amazone_Review_ETL.py ./data/Amazon_5_Core_Review/9_batch Amazon_Product<br>
> \>spark-submit Amazone_Review_ETL.py ./data/Amazon_5_Core_Review/10_batch Amazon_Product<br>
> \>spark-submit Amazone_Review_ETL.py ./data/Amazon_5_Core_Review/11_batch Amazon_Product<br>
> \>spark-submit Amazone_Review_ETL.py ./data/Amazon_5_Core_Review/12_batch Amazon_Product

After these 12 applications done, in project folder they will generate two folders named "Amazon_Product_Review_Parquet" and "Amazon_Product_Review_Json", and each of them have 60 files.
<br>
<br>

3. Execute below commands to move $\frac{1}{5}$ of 60 files from each folders for other applications since they are too huge
>\>cd ~/home/bigdata/assignment/project/cmpt732/data/<br>
>\>mkdir Amazon_Product_Review_Parquet_Part_00000<br>
>\>mkdir Amazon_Product_Review_Json_Part_00000<br>
>\>mv ./Amazon_Product_Review_Parquet/part-00000* Amazon_Product_Review_Parquet_Part_00000/<br>
>\>mv ./Amazon_Product_Review_Json/part-00000* Amazon_Product_Review_Json_Part_00000/

From now on, we have basic data for all other applications in "Amazon_Product_Review_Parquet_Part_00000" and "Amazon_Product_Review_Json_Part_00000/".
<br>
<br>

# 4. Running applications
<br>

## 1). Market basket recommendation
>\>cd ~/home/bigdata/assignment/project/cmpt732/<br>
>\>spark-submit Market_Basket_Analysis.py ./data/Amazon_Product_Review_Parquet_Part_00000

OR
>\>spark-submit Market_Basket_Analysis.py ./testdata/Amazon_Product_Review_Parquet_Part_00000

It will create a folder named "Basket_Recommendation" in folder data/ and populate 24 parquet files in it. This include a table with product A and product B and Support/Confidence/Lift for them.

## 2). Predict whether a customer will purchase a good
>\>cd ~/home/bigdata/assignment/project/cmpt732/<br>
>\>spark-submit Customer_IfPurchasement_Data_Preparation.py ./data/Amazon_Product_Review_Parquet_Part_00000

OR
>\>spark-submit Customer_IfPurchasement_Data_Preparation.py ./testdata/Amazon_Product_Review_Parquet_Part_00000

It will create a folder named "CustomerIfPurchase_Dataset" in folder data/, and one json file in it, use it to replace below JSON_FILE_NAME.

>\>python3 Customer_IfPurchasement.py ./data/CustomerIfPurchase_Dataset/JSON_FILE_NAME

It will populate a SVM model stored in folder cmpt732, this can be loaded for other needs.