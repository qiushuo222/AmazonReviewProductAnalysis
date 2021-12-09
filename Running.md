# 1. Running Envrionment

Python 3.5+, Spark 3.0+ with hadoop 3.2, Linux system
<br>
<br>

# 2. Project Code repository

Gitlab link: <br>
https://csil-git1.cs.surrey.sfu.ca/hza168/cmpt732/-/tree/master

Git Clone to local:

> \>git clone git@csil-git1.cs.surrey.sfu.ca:hza168/cmpt732.git<br> >\>cd cmpt732<br> >\>mkdir data<br> >\>pwd<br>
> ~/home/bigdata/assignment/project/cmpt732

All below steps in Data Preparation is under assumption that working under local standalone Spark in folder ~/home/bigdata/assignment/project/cmpt732.

However, all of original dataset and intermediate results during ETL are also avaliable on cluster in below folder if want to check manually:

> \/home/sqa13/cmpt732_project_data/

<br>
<br>

# 3. Data Preparation

This preparation of data is going to be a huge workload for Spark and memory consuming, so we have prepared small data set for instructors and TAs for testing purpose of next chapter's application, they are available in testdata folder of git repository if you want to skip this tep. Meanwhile, all of intermediate and result are also available in /home/sqa13/cmpt732_project_data/ on cluster.

<br>

## 1). Data Source

Download Amazon product metadata (11.5GB) and 5-core Review data (13.6GB) from below link:

> https://nijianmo.github.io/amazon/index.html#complete-data

Put metadata and 5-core Review data into two folders: Amazon_Product_Ori and Amazon_5_Core_Review_Ori

P.S. Both Amazon_Product_Ori and Amazon_5_Core_Review_Ori are also avalable in /home/sqa13/cmpt732_project_data/ on cluster, but they are too big to put into HDFS.
<br>
<br>

## 2). Pre-process for Spark

1. Put two downloaded folder in local file system:
   > ~/home/bigdata/assignment/project/cmpt732/data/Amazon_Product_Ori<br>
   > ~/home/bigdata/assignment/project/cmpt732/data/Amazon_5_Core_Review_Ori<br>

<br>

2. Go into pyspark shell (with driver memory 4g if running standalone spark) and input below command:
   > \>cd ~/home/bigdata/assignment/project/cmpt732/<br> >\>pyspark --conf spark.driver.memory=4g<br>

```
Path = "./data/"
Product_data_path = Path + "Amazon_Product_Ori"
Review_data_path = Path + "Amazon_5_Core_Review_Ori"
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
> "~/home/bigdata/assignment/project/cmpt732/data/Amazon_5_Core_Review"<br>

P.S. Amazon_Product is avalable in /home/sqa13/cmpt732_project_data/ on cluster.
<br>

3. Divide 5 core review data into 12 batches by input below commands:
   > \>cd ~/home/bigdata/assignment/project/cmpt732/data/Amazon_5_Core_Review<br> > \>mkdir 777 {1..12}\_batch<br> > \>mv part-0000[0-5].gz ./1_batch<br> > \>mv part-0000[6-9].gz ./2_batch<br> > \>mv part-0001[0-1].gz ./2_batch<br> > \>mv part-0001[2-7].gz ./3_batch<br> > \>mv part-0001[8-9].gz ./4_batch<br> > \>mv part-0002[0-3].gz ./4_batch<br> > \>mv part-0002[4-9].gz ./5_batch<br> > \>mv part-0003[0-5].gz ./6_batch<br> > \>mv part-0003[6-9].gz ./7_batch<br> > \>mv part-0004[0-1].gz ./7_batch<br> > \>mv part-0004[2-7].gz ./8_batch<br> > \>mv part-0004[8-9].gz ./9_batch<br> > \>mv part-0005[0-3].gz ./9_batch<br> > \>mv part-0005[4-9].gz ./10_batch<br> > \>mv part-0006[0-5].gz ./11_batch<br> > \>mv part-0006[6-9].gz ./12_batch<br> > \>mv part-0007[0-1].gz ./12_batch<br>

Due to review data is too huge and during ETL we need join product and review data, we have to deal with part of review data firstly one by one.

P.S. Amazon_5_Core_Review is avalable in /home/sqa13/cmpt732_project_data/ on cluster.

<br>

## 3). Basic ETL

1. Cleaning, Transformation, Load for Amazon product data (Consider increase driver or executor memory when execute spark-submit)
   > \>cd ~/home/bigdata/assignment/project/cmpt732/<br> > \>spark-submit --conf spark.driver.memory=2g Amazon_Product_ETL.py ./data/Amazon_Product ./data/Amazon_Product_Parquet

After this application done, in data folder it will generate a folder named "Amazon_Product_Parquet", and 72 parquet files within it.

P.S. Amazon_Product_Parquet is available in /home/sqa13/cmpt732_project_data/ on cluster.
<br>
<br>

2. Cleaning, Transformation, Load for Amazon review data, plus combine product and review information together<br>

> \>cd ~/home/bigdata/assignment/project/cmpt732/<br> > \>spark-submit --conf spark.driver.memory=2g Amazon_Review_ETL.py ./data/Amazon_5_Core_Review/1_batch ./data/Amazon_Product_Parquet ./data/<br> > \>spark-submit --conf spark.driver.memory=2g Amazon_Review_ETL.py ./data/Amazon_5_Core_Review/2_batch ./data/Amazon_Product_Parquet ./data/<br> > \>spark-submit --conf spark.driver.memory=2g Amazon_Review_ETL.py ./data/Amazon_5_Core_Review/3_batch ./data/Amazon_Product_Parquet ./data/<br> > \>spark-submit --conf spark.driver.memory=2g Amazon_Review_ETL.py ./data/Amazon_5_Core_Review/4_batch ./data/Amazon_Product_Parquet ./data/<br> > \>spark-submit --conf spark.driver.memory=2g Amazon_Review_ETL.py ./data/Amazon_5_Core_Review/5_batch ./data/Amazon_Product_Parquet ./data/<br> > \>spark-submit --conf spark.driver.memory=2g Amazon_Review_ETL.py ./data/Amazon_5_Core_Review/6_batch ./data/Amazon_Product_Parquet ./data/<br> > \>spark-submit --conf spark.driver.memory=2g Amazon_Review_ETL.py ./data/Amazon_5_Core_Review/7_batch ./data/Amazon_Product_Parquet ./data/<br> > \>spark-submit --conf spark.driver.memory=2g Amazon_Review_ETL.py ./data/Amazon_5_Core_Review/8_batch ./data/Amazon_Product_Parquet ./data/<br> > \>spark-submit --conf spark.driver.memory=2g Amazon_Review_ETL.py ./data/Amazon_5_Core_Review/9_batch ./data/Amazon_Product_Parquet ./data/<br> > \>spark-submit --conf spark.driver.memory=2g Amazon_Review_ETL.py ./data/Amazon_5_Core_Review/10_batch ./data/Amazon_Product_Parquet ./data/<br> > \>spark-submit --conf spark.driver.memory=2g Amazon_Review_ETL.py ./data/Amazon_5_Core_Review/11_batch ./data/Amazon_Product_Parquet ./data/<br> > \>spark-submit --conf spark.driver.memory=2g Amazon_Review_ETL.py ./data/Amazon_5_Core_Review/12_batch ./data/Amazon_Product_Parquet ./data/

After these 12 applications done, in data folder they will generate two folders named "Amazon_Product_Review_Parquet" and "Amazon_Product_Review_Json", and each of them have 60 files.

P.S. Amazon_Product_Review_Parquet and Amazon_Product_Review_Json are available in /home/sqa13/cmpt732_project_data/ on cluster.
<br>
<br>

3. Execute below commands to move $\frac{1}{5}$ of 60 files from each folders for other applications since they are too huge
   > \>cd ~/home/bigdata/assignment/project/cmpt732/data/<br> >\>mkdir Amazon_Product_Review_Parquet_Part_00000<br> >\>mkdir Amazon_Product_Review_Json_Part_00000<br> >\>mv ./Amazon_Product_Review_Parquet/part-00000* Amazon_Product_Review_Parquet_Part_00000/<br> >\>mv ./Amazon_Product_Review_Json/part-00000* Amazon_Product_Review_Json_Part_00000/

From now on, we have basic data for all other applications in "Amazon_Product_Review_Parquet_Part_00000" and "Amazon_Product_Review_Json_Part_00000".

P.S. Both Amazon_Product_Review_Parquet_Part_00000 and Amazon_Product_Review_Json_Part_00000 are available in /home/sqa13/cmpt732_project_data/ on cluster.

<br>
<br>

# 4. Running applications

## 1). Market basket recommendation

> \>cd ~/home/bigdata/assignment/project/cmpt732/<br> >\>spark-submit --conf spark.driver.memory=2g Market_Basket_Analysis.py ./data/Amazon_Product_Review_Parquet_Part_00000 ./testdata/Basket_Recommendation

OR for Instructors or TAs testing

> \>spark-submit Market_Basket_Analysis.py ./testdata/Amazon_Product_Review_Parquet_Part_00000 ./testdata/Basket_Recommendation

It will create a folder named "Basket_Recommendation" in folder testdata and populate 24 parquet files in it. This include a table with product A and product B and Support/Confidence/Lift for them.

P.S. The Basket_Recommendation is available in /home/sqa13/cmpt732_project_data/ on cluster.

## 2). Predict whether a customer will purchase a good

> \>cd ~/home/bigdata/assignment/project/cmpt732/<br> >\>spark-submit Customer_IfPurchasement_Data_Preparation.py ./data/Amazon_Product_Review_Parquet_Part_00000 ./testdata/CustomerIfPurchase_Dataset

OR for Instructors or TAs testing

> \>spark-submit Customer_IfPurchasement_Data_Preparation.py ./testdata/Amazon_Product_Review_Parquet_Part_00000 ./testdata/CustomerIfPurchase_Dataset

It will create a folder named "CustomerIfPurchase_Dataset" in folder testdata, and one json file in it, use json file name to replace below JSON_FILE_NAME.

> \>python3 Customer_IfPurchasement.py ./testdata/CustomerIfPurchase_Dataset/JSON_FILE_NAME ./testdata/Customer_IfPurchasement_SVM_Model

It will populate a SVM model stored in folder testdata, this can be loaded for other needs.

P.S. The CustomerIfPurchase_Dataset and trained model file is available in /home/sqa13/cmpt732_project_data/ on cluster.

## 3). Seasonal Sales Count and Prediction

> \>cd ~/home/bigdata/assignment/project/cmpt732/<br> >\>spark-submit --conf spark.driver.memory=2g Category_Seasonal_Analysis.py ./data/Amazon_Product_Review_Parquet_Part_00000 ./testdata/

OR for Instructors or TAs testing

> \>spark-submit Category_Seasonal_Analysis.py ./testdata/Amazon_Product_Review_Parquet_Part_00000 ./testdata/

It will create a new pie chart plot named Seasonal_Analysis_Pie_Chart in folder testdata shows the market share of each category in four seasons. The trained model will also be saved to ./testdata/ folder with name Category_Prediction_Model. Also, the training result of the model will be printed out in the terminal.

To use the model for testing purpose, run:

> \>spark-submit Category_Seasonal_Prediction.py ./testdata/Category_Prediction_Model ./testdata/Amazon_Product_Review_Parquet_Part_00000

The prediction result will be shown in the terminal.

P.S. The plot Seasonal_Analysis_Pie_Chart.png and trained model Category_Prediction_Model can be found at /home/sqa13/cmpt732_project_data/

## 4). Repurchase Rate Analysis

> \>cd ~/home/bigdata/assignment/project/cmpt732/<br> >\>spark-submit RePurchase_Analysis.py ./data/Amazon_Product_Review_Parquet_Part_00000 ./testdata/

OR for Instructors or TAs testing

> \>spark-submit RePurchase_Analysis.py ./testdata/Amazon_Product_Review_Parquet_Part_00000 ./testdata/

It will create a plot named Repurchase_Rate in folder testdata shows the repurchase rate in each category.

P.S. The plot Repurchase_Rate.png can be found at /home/sqa13/cmpt732_project_data/

## 5). Product Category Prediction

> \>cd ~/home/bigdata/assignment/project/cmpt732/<br> >\>spark-submit --conf spark.driver.memory=2g Category_Prediction_Train.py ./testdata/Amazon_Product_Review_Parquet_Part_00000 ./testdata/category_prediction

OR for Instructors or TAs testing

> \>spark-submit Category_Prediction_Train.py ./testdata/Amazon_Product_Review_Parquet_Part_00000 ./testdata/category_prediction

It will train a model to predict which category the product belong to based on its title, description, and brand. The training result of the model will be printed out in the terminal.

> \>cd ~/home/bigdata/assignment/project/cmpt732/<br> >\>spark-submit --conf spark.driver.memory=2g Category_Prediction.py ./testdata/category_prediction ./testdata/Amazon_Product_Review_Parquet_Part_00000

OR for Instructors or TAs testing

> \>spark-submit Category_Prediction.py ./testdata/category_prediction ./testdata/Amazon_Product_Review_Parquet_Part_00000

It will appied the trained model to predict which category the product belong to based on its title, description, and brand. The predicting of the data will be printed out in the terminal.

P.S. The trained model file is available in /home/sqa13/cmpt732_project_data/ on cluster.

## 6). Views Amount Analysis

> \>cd ~/home/bigdata/assignment/project/cmpt732/<br> >\>spark-submit Views_Count_Plot.py ./data/Amazon_Product_Review_Parquet_Part_00000 ./testdata/

OR for Instructors or TAs testing

> \>spark-submit Views_Count_Plot.py ./testdata/Amazon_Product_Review_Parquet_Part_00000 ./testdata/

It will create plots named "Popularity of views pie", "Amount of views bar", "Popularity of products pie", and "Amount of products bar" in folder testdata shows the views amount in each category.

P.S. The plot "Amount of products bar.png", "Amount of views bar.png", "Popularity of products pie.png", and "Popularity of views pie.png" are available in /home/sqa13/cmpt732_project_data/ on cluster.

## 7). Sales per Months Analysis

> \>cd ~/home/bigdata/assignment/project/cmpt732/<br> >\>spark-submit --conf spark.driver.memory=3g Sales_Months_Analysis.py ./data/Amazon_Product_Review_Parquet_Part_00000 ./testdata/

OR for Instructors or TAs testing

> \>spark-submit --conf spark.driver.memory=3g Sales_Months_Analysis.py ./testdata/Amazon_Product_Review_Parquet_Part_00000 ./testdata/

It will create five bar charts, namely "sales_month_Amazon_Devices.png", "sales_month_Amazon_Home.png", "sales_month_Computers.png", "sales_month_Grocery.png", and "sales_month_Sports_Outdoors.png" in folder testdata.

## 8). Price Distribution Analysis

> \>cd ~/home/bigdata/assignment/project/cmpt732/<br> >\>spark-submit Price_Distribution.py ./data/Amazon_Product_Review_Parquet_Part_00000 ./testdata/

OR for Instructors or TAs testing

> \>spark-submit Price_Distribution.py ./testdata/Amazon_Product_Review_Parquet_Part_00000 ./testdata/

It will create two boxplots and one scatter plots, namely "price_boxplot.png", "price_boxplot_no_outliers.png", and "avg_price_distribution_Home.png" in folder testdata.

## 9). Review Analysis

> \>cd ~/home/bigdata/assignment/project/cmpt732/<br> >\>spark-submit Review_Analysis.py ./data/Amazon_Product_Review_Parquet_Part_00000 ./testdata/

OR for Instructors or TAs testing

> \>spark-submit Review_Analysis.py ./testdata/Amazon_Product_Review_Parquet_Part_00000 ./testdata/

It will generate a wordcloud plot named "wordcloud.png" in the testdata folder.
