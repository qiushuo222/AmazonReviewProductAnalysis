import sys
assert sys.version_info >= (3, 5) 

from pyspark.sql import SparkSession, functions, types
import matplotlib.pyplot as plt
from pyspark.sql.functions import regexp_replace
import numpy as np
import os

def main(inputs, output):

    data = spark.read.parquet(inputs)  # multiple inputs
    data_combined = data.withColumn("Categories",regexp_replace('Product_Main_Category', '&amp;','&'))
    # data_combined.printSchema()

    data_m = data_combined.withColumn('month', functions.month(data['Review_Post_Date']))
    data_ym = data_m.withColumn('year', functions.year(data['Review_Post_Date']))
    ## 1999 has only month 6,7,10,11,12, delete 1999 data.
    data_no_1999 = data_ym.filter(data_ym['year']!=1999)
    # category counts
    category_count = data_no_1999.groupby('Categories').count()  
    # category_count.show()
    # category list
    category = category_count.select('Categories').collect()
    category_list = [str(Row['Categories']) for Row in category]

    '''
    Categories list:

    ['Computers', 'All Electronics', 'GPS & Navigation', 'Home Audio & Theater', 'Pet Supplies', 'Toys & Games', 
    'Baby', 'Sports & Outdoors', 'Grocery', 'Video Games', 'Automotive', 'Books', 'Amazon Home', 'Industrial & Scientific', 
    'Health & Personal Care', 'Cell Phones & Accessories', 'Arts, Crafts & Sewing', 'Amazon Devices', 'Car Electronics', 
    'Tools & Home Improvement', 'All Beauty', 'Office Products', 'Appliances', 'Camera & Photo', 'Apple Products', 
    'Musical Instruments', 'Portable Audio & Accessories']

    '''


    '''

    Price distribution in each category

    '''
    
    category_data = data_no_1999.filter(data_combined['Categories'] == 'Amazon Home')
    data_no_null = category_data.filter(category_data['Product_Price'].isNull() != True)  # get 208011 not null price 
    ## a giplise of 'Amazon Home' data
    prices_pd = data_no_null.select('Product_Price').toPandas()
    print('Price distribution of products in Amazon Home category: ')
    prices_pd.describe()
    '''
    Product_Price
    count  208011.000000
    mean       24.271111
    std        47.635906
    min         0.010000
    25%         8.790000
    50%        14.970000
    75%        24.600000
    max      3299.000000
    '''
    ## boxplot
    home_prices = np.array(data_no_null.select('Product_Price').collect()).reshape(-1)
    # plt.figure()
    plt.boxplot(home_prices)
    plt.title('Price Distribution for Amazon Home Category')
    plt.savefig(output + 'price_boxplot.png')
    plt.show()

    ## boxplot w/o outliers
    # plt.figure()
    plt.boxplot(home_prices, showfliers=False)
    plt.title('Price Distribution for Amazon Home Category (without Outliers)')
    plt.savefig(output + 'price_boxplot_no_outliers.png')
    plt.show()

    # home = data_no_null.groupBy('Product_Brand').count().orderBy('count', ascending=False) 
    h_counts = data_no_null.groupBy('Product_Brand').count().orderBy('Product_Brand') 
    h_avgprice = data_no_null.groupBy('Product_Brand').avg('Product_Price').orderBy('Product_Brand')
    h = h_counts.join(h_avgprice, on='Product_Brand').orderBy('count', ascending=False)
    h_ = h.withColumn('average_price', functions.round(functions.col('avg(Product_Price)'),2))
    # get pyspark dataframe with 'Brand', 'sales_count', 'avg_price'
    avgprice_count = h_.select('Product_Brand', h_['count'].alias('sales_count'), 'average_price')
    # avg_prices = data_no_null.groupBy('Product_Brand').avg('Product_Price').orderBy('avg(Product_Price)', ascending=False) 
    # filter top sale brand 
    top = avgprice_count.filter(avgprice_count['sales_count'] > 1000)
    top_count = top.count()  # 30 top sales brands
    print('Top sold brands & average prices in Amazon Home category:')
    top.show()
    '''
    +--------------------+-----------+-------------+                                
    |       Product_Brand|sales_count|average_price|
    +--------------------+-----------+-------------+
    |                 OXO|      10399|        19.58|
    |           Cuisinart|       8634|        25.28|
    |      Hamilton Beach|       4243|        15.46|
    |              Norpro|       3379|        11.33|
    |               Lodge|       3269|         27.8|
    |              Wilton|       2919|        13.77|
    |          KitchenAid|       2094|        39.96|
    |              Presto|       2039|         8.11|
    |         Instant Pot|       1976|         6.95|
    |        BLACK+DECKER|       1861|        16.01|
    |    New Metro Design|       1786|        21.47|
    |          Victorinox|       1720|        43.41|
    |      Black & Decker|       1700|        12.59|
    |         Progressive|       1561|        12.21|
    |          Kuhn Rikon|       1495|         16.9|
    |         Nordic Ware|       1469|        16.43|
    
    '''
    ## avg_price scatter plot for top brands in "Amazon Home"
    # plt.figure()
    x=np.arange(top_count)
    avgprices = np.array(top.select('average_price').collect()).reshape(-1)
    top_brands = np.array(top.select('Product_Brand').collect()).reshape(-1)
    # area = (30 * np.random.rand(top_count))**2
    area = np.arange(top_count)**2
    area = np.flipud(area)
    colors = np.random.rand(top_count)
    plt.scatter(x, avgprices, c=colors, s=area, alpha=0.5)
    plt.xlabel('Sales Rank')
    plt.ylabel('Average Price')
    plt.title('Average Prices for Top Brands in "Amazon Home" Category')
    for i, brand in enumerate(top_brands[:5]):  # add annotation
        plt.annotate(brand, (x[i], avgprices[i]), fontsize=8)
    plt.savefig(output + 'avg_price_distribution_Home.png')
    plt.show()
        


if __name__ == '__main__':
    # ffolder = os.path.abspath("")
    inputs = sys.argv[1]
    output = sys.argv[2]
    # Path = os.path.join(ffolder, inputs)

    spark = SparkSession.builder.appName('Price distribution for most popular category code').getOrCreate()
    assert spark.version >= '3.0' 
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext

    main(inputs, output)



### submit command:
# spark-submit Price_Distribution.py ./data/Amazon_Product_Review_Parquet_Part_00000 ./testdata/
