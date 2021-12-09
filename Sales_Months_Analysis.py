import os
import sys
assert sys.version_info >= (3, 5) 

from pyspark.sql import SparkSession, functions, types
import matplotlib.pyplot as plt
from pyspark.sql.functions import regexp_replace


def main(inputs, output):

    data = spark.read.parquet(inputs)  # multiple inputs
    # data = spark.read.parquet('/Users/wanyi/Documents/CMPT_732/project_data_parquet/Amazon_Product_Review_Parquet/part-00000-*.parquet', '/Users/wanyi/Documents/CMPT_732/project_data_parquet/Amazon_Product_Review_Parquet/part-00001-*.parquet', '/Users/wanyi/Documents/CMPT_732/project_data_parquet/Amazon_Product_Review_Parquet/part-00002-*.parquet', '/Users/wanyi/Documents/CMPT_732/project_data_parquet/Amazon_Product_Review_Parquet/part-00003-*.parquet', '/Users/wanyi/Documents/CMPT_732/project_data_parquet/Amazon_Product_Review_Parquet/part-00004-*.parquet')
    type(data)
    # data.show(15)
    data.printSchema()

    # combine the same categories (with different naming format & and &amp;)
    data_combined = data.withColumn("Categories",regexp_replace('Product_Main_Category', '&amp;','&'))  # reduce from 36 categories to 27
    
    catgory_counts = data_combined.groupby('Categories').count()
    # data1 = catgory_counts.orderBy('count')
    # catgory_counts.show()
    data_m = data_combined.withColumn('month', functions.month(data['Review_Post_Date']))
    data_ym = data_m.withColumn('year', functions.year(data['Review_Post_Date']))
    # data_ym.select('Review_Post_Date', 'year', 'month').show(5)  # check date, year, month
    # data_ym.filter(data_ym['month']==12).groupby('year','month').count().orderBy('year').show(40) # check count in each month
    ## year (1999 - 2017), month (all 12)
    ## 1999 has only month 6,7,10,11,12, delete 1999 data.
    data_no_1999 = data_ym.filter(data_ym['year']!=1999)
    data_no_1999.cache()
    # category counts
    category_count = data_no_1999.groupby('Categories').count()  
    category_count.show()
    # category list
    category = category_count.select('Categories').collect()
    category_list = [str(Row['Categories']) for Row in category]
    # month list
    months = data_no_1999.groupby('month').count().orderBy('month').select('month').collect()
    months_list = [str(Row['month']) for Row in months] 
    # year list
    yr = data_no_1999.groupby('year').count().orderBy('year').select('year').collect()
    yr_list = [int(Row['year']) for Row in yr] 
    yr_span = yr_list[-1] - yr_list[0]  # 18-year span

    '''
    
    +--------------------+------+
    |          Categories| count|
    +--------------------+------+
    |      Apple Products|    15|
    |                Baby|    73|
    |               Books|    89|
    |          All Beauty|   175|
    |         Video Games|   420|
    |    GPS & Navigation|   564|
    |          Appliances|   616|
    |Health & Personal...|   620|
    |      Amazon Devices|   766|
    |             Grocery|   932|
    |Portable Audio & ...|  1106|
    |Arts, Crafts & Se...|  2152|
    |        Pet Supplies|  2229|
    |        Toys & Games|  4215|
    |Industrial & Scie...|  4545|
    |   Sports & Outdoors|  5219|
    |Cell Phones & Acc...|  5948|
    |     Car Electronics|  7140|
    | Musical Instruments| 17313|
    |     Office Products| 56574|
    |      Camera & Photo| 69467|
    |Home Audio & Theater| 92259|
    |     All Electronics|110477|
    |          Automotive|118182|
    |Tools & Home Impr...|136654|
    |           Computers|141325|
    |         Amazon Home|281143|
    +--------------------+------+

    '''
    '''
    Categories list:

    ['Computers', 'All Electronics', 'GPS & Navigation', 'Home Audio & Theater', 'Pet Supplies', 'Toys & Games', 
    'Baby', 'Sports & Outdoors', 'Grocery', 'Video Games', 'Automotive', 'Books', 'Amazon Home', 'Industrial & Scientific', 
    'Health & Personal Care', 'Cell Phones & Accessories', 'Arts, Crafts & Sewing', 'Amazon Devices', 'Car Electronics', 
    'Tools & Home Improvement', 'All Beauty', 'Office Products', 'Appliances', 'Camera & Photo', 'Apple Products', 
    'Musical Instruments', 'Portable Audio & Accessories']

    '''

    '''

    Average number of sales vs. Month (over year 2000-2018)

    '''
    ## here, we define the number of reviews as # records of purchase once -> sales
    # select any category you want
    category_selected = ['Sports & Outdoors', 'Grocery', 'Amazon Devices', 'Amazon Home', 'Computers']
    for c in category_selected:
        c1 = c.replace('&', ' ')
        c2 = c1.replace(',', ' ')
        c_list = c2.split()
        new_c = '_'.join(c_list)  # strip space and special characters
        category_data = data_no_1999.filter(data_combined['Categories'] == c).cache()
        category_data_1 = category_data.groupby('month').count().orderBy('month')  # total sales distribution each month over years
        category_data_2 = category_data_1.withColumn('avg_count_float', category_data_1['count']/yr_span)
        category_data_3 = category_data_2.withColumn('avg_count', functions.ceil(functions.col('avg_count_float')))  # int for #sales
        category_pd = category_data_3.toPandas()  # convert to pandas dataframe
        # plot
        category_pd.plot.bar(x='month', y='avg_count', width=0.9)
        plt.title(f'Average Sales for {new_c} Category (Year 2000-2018)')
        plt.xlabel('Month')
        plt.ylabel('Average Number of Sales Per Month')
        plt.xticks(rotation='horizontal')
        plt.savefig(output + f'sales_month_{new_c}.png')
        plt.show()
          

if __name__ == '__main__':
    # ffolder = os.path.abspath("")
    inputs = sys.argv[1]
    output = sys.argv[2]
    # Path = os.path.join(ffolder, inputs)

    spark = SparkSession.builder.appName('Average sales & month for different categories code').getOrCreate()
    assert spark.version >= '3.0' 
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)


### submit command:
# spark-submit --conf spark.driver.memory=3g Sales_Months_Analysis.py ./data/Amazon_Product_Review_Parquet_Part_00000 ./testdata/
