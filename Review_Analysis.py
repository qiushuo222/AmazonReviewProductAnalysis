import sys
import os
assert sys.version_info >= (3, 5) 
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import regexp_replace
import nltk
from nltk.stem.snowball import SnowballStemmer
from nltk.tokenize import word_tokenize
import re
from wordcloud import WordCloud
import matplotlib.pyplot as plt

def show_wordcloud(data, output):
    wordcloud = WordCloud(
        background_color='white',
        width = 800, 
        height = 400, 
        max_words = 200,
        max_font_size = 100,
        scale = 3,
        random_state = 42
    ).generate(str(data))
    plt.axis('off')
    # plt.figure()
    # plt.title('Top Words in Top Product Reviews')
    plt.imshow(wordcloud)
    # plt.savefig('/Users/wanyi/Documents/CMPT_732/project/cmpt732/wordcloud/wordcloud.png')
    plt.savefig(output + 'wordcloud.png')
    plt.show()

def main(inputs, output):
    # data = spark.read.parquet(inputs)
    data = spark.read.parquet(inputs)
    # data.cache()
    # data.printSchema()
    data_combined = data.withColumn("Categories",regexp_replace('Product_Main_Category', '&amp;','&'))
    # data_combined.cache() 

    ## check "Amazon Devices" category
    amazon_home = data_combined.filter(data_combined['Categories']=='Amazon Home')
    # print('average rate for different products in Amazon Home category:')
    # amazon_home.groupby('Product_Asin').avg('Rate').orderBy('avg(Rate)').show(10)  # average rate check
    print('Top sales star products in Amazon Home category:')
    amazon_home.groupby('Product_Asin').count().orderBy('count', ascending=False).show(10)  # check top sales product
    '''
    +------------+-----+
    |Product_Asin|count|
    +------------+-----+
    |  B00FLYWNYQ| 1776|
    |  B0015TMHSI| 1213|
    |  B00008UA3Y| 1023|
    |  B000067DW6|  815|
    |  B00018RRRK|  781|
    |  B00005MF9C|  719|
    '''

    star_home = amazon_home.filter(amazon_home['Product_Asin']== 'B00FLYWNYQ')  # filter top star product data
    star_review = star_home.filter(star_home['Review_Content'].isNull() != True)  # filter null review content -> 1775
    star_review.cache()
    avg_rate = star_home.groupby('Product_Asin').avg('Rate') # 4.72 average rate
    # check which star product: 
    title = star_home.groupby('Product_Title').count().select('Product_Title').collect()  
    title_lst = [str(Row['Product_Title']) for Row in title][0]  # product title: 'Instant Pot DUO60 6 Qt 7-in-1 Multi-Use Programmable Pressure Cooker, Slow Cooker, Rice Cooker, Steamer, Saut&eacute;, Yogurt Maker and Warmer'
    print('Top sales star product title:')
    print(title_lst)

    # what reviews say about this star product
    reviews = star_review.select('Review_Content')  # still pyspark dataframe
    # reviews.write.csv(output, mode='overwrite')  # write reviews to csv file
    reviews_pd = reviews.toPandas()
    reviews_list0 = reviews_pd.values.tolist()
    reviews_list = [x[0] for x in reviews_list0]
    # reviews_rdd = reviews.rdd  # convert to rdd
    # review = reviews_rdd.map(lambda x: x[0])

    ## tokenization and stemming
    stopwords = nltk.corpus.stopwords.words('english')
    # print(stopwords)
    stemmer = SnowballStemmer("english")
    words = []
    for text in reviews_list:
        text = text.lower()
        text = word_tokenize(text) # tokenize to words
        filtered_tokens = []
        for i in text:
            if re.search('[a-zA-Z]', i):
                filtered_tokens.append(i)
        texts = [x for x in filtered_tokens if x not in stopwords]
        # print(texts)
        texts_clean = [stemmer.stem(j) for j in texts]
        # print(texts_clean)
        words.append(texts_clean)
    # print(words[:3])

    ## wordcloud
    word_list = [item for sublist in words for item in sublist]
    # print(word_list)
    words_lst = (' ').join(word_list)
    show_wordcloud(words_lst, output)


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    # ffolder = os.path.abspath("")
    # Path = os.path.join(ffolder, inputs)

    spark = SparkSession.builder.appName('Review analysis for most popular product in top category code').getOrCreate()
    assert spark.version >= '3.0' 
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    
    main(inputs, output)



### run on local, since cannot load nltk on cluster
# spark-submit Review_Analysis.py ./data/Amazon_Product_Review_Parquet_Part_00000 ./testdata/
