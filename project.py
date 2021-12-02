import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
import matplotlib.pyplot as plt

# add more functions as necessary
def yr_mon(date_data):
    year = functions.year(date_data)
    month = functions.month(date_data)
    return (year, month)


def main(inputs):

    data = spark.read.parquet(inputs)  # multiple inputs
    # data = spark.read.parquet('/Users/wanyi/Documents/CMPT_732/project_data_parquet/Amazon_Product_Review_Parquet/part-00000-*.parquet', '/Users/wanyi/Documents/CMPT_732/project_data_parquet/Amazon_Product_Review_Parquet/part-00001-*.parquet', '/Users/wanyi/Documents/CMPT_732/project_data_parquet/Amazon_Product_Review_Parquet/part-00002-*.parquet', '/Users/wanyi/Documents/CMPT_732/project_data_parquet/Amazon_Product_Review_Parquet/part-00003-*.parquet', '/Users/wanyi/Documents/CMPT_732/project_data_parquet/Amazon_Product_Review_Parquet/part-00004-*.parquet')
    data.cache()
    type(data)
    # data.show(15)
    data.printSchema()
    
    data1 = data.groupby('Product_Main_Category').count()
    # data1 = data1.orderBy('count')
    data1.show()
    data2 = data.withColumn('month', functions.month(data['Review_Post_Date']))
    data2 = data2.withColumn('year', functions.year(data['Review_Post_Date']))
    data2.select('Review_Post_Date', 'year', 'month').show(5)  # check date, year, month
    data2.filter(data2['month']==12).groupby('year','month').count().orderBy('year').show(40) # check count in each month
    ## year (1999 - 2017), month (all 12)
    ## 1999 has only month 6,7,10,11,12, delete 1999 data.
    data_no_1999 = data2.filter(data2['year']!=1999)
    data_no_1999.cache()
    # category counts
    category_count = data_no_1999.groupby('Product_Main_Category').count()  #.show()
    category_count.show()
    # category list
    category = category_count.select('Product_Main_Category').collect()
    category_list = [str(Row['Product_Main_Category']) for Row in category]
    # month list
    m = data_no_1999.groupby('month').count().orderBy('month').select('month').collect()
    m_list = [str(Row['month']) for Row in m] 
    # year list
    yr = data_no_1999.groupby('year').count().orderBy('year').select('year').collect()
    yr_list = [int(Row['year']) for Row in yr] 
    yr_span = yr_list[-1] - yr_list[0]  # 18-year span

    '''
    Average number of sales vs. Month (over year 2000-2018)

    '''
    ## here, we define the number of reviews as # records of purchase once -> sales
    for c in category_list:
        category_data = data_no_1999.filter(data['Product_Main_Category'] == c).cache()
        category_data_1 = category_data.groupby('month').count().orderBy('month')  # total sales distribution each month over years
        category_data_2 = category_data_1.withColumn('avg_count_float', category_data_1['count']/yr_span)
        category_data_3 = category_data_2.withColumn('avg_count', functions.ceil(functions.col('avg_count_float')))  # int for #sales
        category_pd = category_data_3.toPandas()  # convert to pandas dataframe
        # plot
        category_pd.plot.bar(x='month', y='avg_count', width=0.9)
        plt.title(f'Average Sales for {c} Category (Year 2000-2018)')
        plt.xlabel('Month')
        plt.ylabel('Average Number of Sales Per Month')
        plt.xticks(rotation='horizontal')
        plt.show()
        plt.savefig(f'sales_month_{c}.png')

    # Sport category 
    sport_data = data_no_1999.filter(data['Product_Main_Category'] == 'Sports & Outdoors')  # 5069 rows (sales)
    sport_data1 = sport_data.groupby('month').count().orderBy('month')  # sports products distribution each month over years
    sport_data2 = sport_data1.withColumn('avg_count_float', sport_data1['count']/yr_span)
    sport_data3 = sport_data2.withColumn('avg_count', functions.ceil(functions.col('avg_count_float')))

    
    '''

    Price distribution in each category

    '''
    for c in category_list:
        category_data = data_no_1999.filter(data['Product_Main_Category'] == c).cache()
        
   


if __name__ == '__main__':
    inputs = sys.argv[1]
    # output = sys.argv[2]
    spark = SparkSession.builder.appName('example code').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs)

## submit cml:
# spark-submit project.py parquet_00000