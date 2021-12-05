import sys

from pandas.core.indexes.api import all_indexes_same
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import json
from pyspark.sql import SparkSession, functions, types
import os
from pyspark import SparkConf, SparkContext
import json
import datetime
import re

'''
Market Basket Analysis
Input ==> Amazon product and review parquet
Output ==> Support, Confidence, and Lift value for two related products
'''

def main(Amazon_Product_Review_Path):
    
    df_origin = spark.read.parquet(Amazon_Product_Review_Path)
    
    ####################################################
    ###########Calculate Support Metric P(AB)###########
    ####################################################
    df_Inter = df_origin.select("Reviewer_ID", "Product_Asin", "Product_Purchased").orderBy("Reviewer_ID")
    # Get actual purchased products, 114,229 products
    df_Inter = df_Inter.filter(functions.col("Product_Purchased") == 1).select("Reviewer_ID", "Product_Asin").cache()

    # Self join to get combination of products that been ordered multiple times by different reviewers
    df_join = df_Inter.alias("df1").join(df_Inter.alias("df2"), "Product_Asin", "left").where("df1.Reviewer_ID < df2.Reviewer_ID").select("Product_Asin", functions.col("df1.Reviewer_ID").alias("User1"), functions.col("df2.Reviewer_ID").alias("User2")).distinct()

    # Get list of products and count times for each product combination
    df_join_group = df_join.groupBy("User1", "User2").agg(functions.collect_list("Product_Asin").alias("Product_List"), functions.count("Product_Asin").alias("count")).filter(functions.col("count") > 1)
  
    # Get Support Metric P(AB) for multiple products combination
    df_join_group_support = df_join_group.groupBy("Product_List").sum("count").select("Product_List", functions.col("sum(count)").alias("Support")).cache()

    df_join_group_support_g_2 = df_join_group_support.filter(functions.size("Product_List") > 2)

    df_join_group_support_e_2 = df_join_group_support.filter(functions.size("Product_List") == 2)

    # Transfer >2 combination to 2 combination
    df_join_group_support_g_2 = df_join_group_support_g_2.select(functions.col("Product_List").alias("list1"), functions.col("Product_List").alias("list2"), "Support")
    df_join_group_support_g_2 = df_join_group_support_g_2.select(functions.explode("list1").alias("list1"), "list2", "Support")
    df_join_group_support_g_2 = df_join_group_support_g_2.select("list1", functions.explode("list2").alias("list2"), "Support").orderBy(functions.asc("list1"), functions.asc("list2"))
    df_join_group_support_g_2 = df_join_group_support_g_2.filter(functions.col("list1") < functions.col("list2")).groupBy(["list1", "list2"]).agg(functions.sum("Support").alias("Support"))
    df_join_group_support_g_2 = df_join_group_support_g_2.select(functions.array("list1").alias("list1"), functions.array("list2").alias("list2"), "Support")
    df_join_group_support_g_2 = df_join_group_support_g_2.withColumn("Product_List", functions.concat("list1", "list2")).select("Product_List", "Support")
    
    # Get completed Support Metrics for product group with only two elements
    df_join_group_support_final = df_join_group_support_e_2.union(df_join_group_support_g_2).cache()

    # 235,834
    Count_All = df_join_group_support_final.agg(functions.sum("Support")).collect()[0][0]
 
    df_join_group_support_final = df_join_group_support_final.withColumn("P(AB)", functions.col("Support") / Count_All)

    '''
    +------------------------+-------+--------------------+
    |Product_List            |Support|P(AB)               |
    +------------------------+-------+--------------------+
    |[B0002JFN60, B00KT7DOR0]|2      |8.480541397762833E-6|
    |[B006JH8T3S, B00JJFQ19Q]|2      |8.480541397762833E-6|
    |[B000I5LACO, B009D79VH4]|6      |2.54416241932885E-5 |
    |[B00LMH8CCS, B0002YKBV2]|2      |8.480541397762833E-6|
    |[B0010B5SF0, B000FAJ0K6]|564    |0.002391512674169119|
    |[B003L1ZYYW, B0002YUX8I]|2      |8.480541397762833E-6|
    |[B00091S0WA, B0013AV4M4]|14     |5.936378978433983E-5|
    |[B0000BZL0U, B00007L6C2]|2      |8.480541397762833E-6|
    |[B0017SZT1E, B00173211U]|2      |8.480541397762833E-6|
    |[B00007E7JU, B006Y0YZZ6]|2      |8.480541397762833E-6|
    |[B00009XVKY, B000EVSLRO]|2      |8.480541397762833E-6|
    |[B00L9X6M6I, B006IT9ZSU]|2      |8.480541397762833E-6|
    |[B00007AKCV, B00CDS9EKE]|6      |2.54416241932885E-5 |
    |[B003B01QSG, B0002DVM8E]|2      |8.480541397762833E-6|
    |[B00008UA3Y, B0011YVZMQ]|2      |8.480541397762833E-6|
    |[B000LNQ39I, B002WI8ZWS]|2      |8.480541397762833E-6|
    |[B00015YORE, B00015YORO]|2      |8.480541397762833E-6|
    |[B005LDLP8W, B00BP5KOPA]|2      |8.480541397762833E-6|
    |[B005SW94B6, B00009MDBU]|2      |8.480541397762833E-6|
    |[B000UO9RJG, B002FYGG5M]|2      |8.480541397762833E-6|
    +------------------------+-------+--------------------+
    '''


    #######################################################################
    ###########Calculate Confidence Metric P(B|A) = P(AB) / P(A)###########
    #######################################################################
    df_r = df_join_group_support_final.select(functions.explode("Product_List").alias("Product_Asin"), "Support", "P(AB)")

    df_confidence_A= df_r.groupBy("Product_Asin").agg(functions.sum("Support").alias("count"))
    df_confidence = df_confidence_A.withColumn("P()", functions.col("count") / Count_All)

    # Get A Product
    df_l_AB = df_join_group_support_final.select(functions.col("Product_List").getItem(0).alias("A"), functions.col("Product_List").getItem(1).alias("B"), "P(AB)").dropDuplicates(["A", "B"])

    df_confidence_A = df_l_AB.join(df_confidence, df_l_AB["A"] == df_confidence["Product_Asin"], "left").select("A", functions.col("P()").alias("P(A)"), "B", "P(AB)")
    df_confidence_AB = df_confidence_A.join(df_confidence, df_confidence_A["B"] == df_confidence["Product_Asin"]).select("A", "P(A)", "B", functions.col("P()").alias("P(B)"), "P(AB)")

    # 10738 data points
    df_confidence_final = df_confidence_AB.select("A", "P(A)", (functions.col("P(AB)") / functions.col("P(A)")).alias("Confidence_A_B"), "B", "P(B)", (functions.col("P(AB)") / functions.col("P(B)")).alias("Confidence_B_A"), "P(AB)")

    '''
    +----------+---------------------+---------------------+----------+---------------------+--------------------+--------------------+
    |A         |P(A)                 |Confidence_A_B       |B         |P(B)                 |Confidence_B_A      |P(AB)               |
    +----------+---------------------+---------------------+----------+---------------------+--------------------+--------------------+
    |B0002JFN60|0.13023567424544383  |6.511688480823077E-5 |B00KT7DOR0|2.798578661261735E-4 |0.030303030303030304|8.480541397762833E-6|
    |B006JH8T3S|0.001568900158586124 |0.005405405405405406 |B00JJFQ19Q|8.480541397762833E-6 |1.0                 |8.480541397762833E-6|
    |B000I5LACO|9.837428021404887E-4 |0.02586206896551724  |B009D79VH4|0.0013992893306308675|0.01818181818181818 |2.54416241932885E-5 |
    |B00LMH8CCS|1.6961082795525667E-5|0.5                  |B0002YKBV2|3.816243628993275E-4 |0.022222222222222223|8.480541397762833E-6|
    |B0010B5SF0|0.005062883214464411 |0.4723618090452262   |B000FAJ0K6|0.005045922131668885 |0.4739495798319328  |0.002391512674169119|
    |B003L1ZYYW|0.005953340061229509 |0.0014245014245014246|B0002YUX8I|2.459357005351222E-4 |0.034482758620689655|8.480541397762833E-6|
    |B00091S0WA|9.328595537539117E-5 |0.6363636363636364   |B0013AV4M4|2.798578661261735E-4 |0.2121212121212121  |5.936378978433983E-5|
    |B0000BZL0U|0.001577380699983887 |0.005376344086021505 |B00007L6C2|8.480541397762833E-6 |1.0                 |8.480541397762833E-6|
    |B0017SZT1E|3.392216559105133E-5 |0.25                 |B00173211U|8.480541397762833E-6 |1.0                 |8.480541397762833E-6|
    |B00007E7JU|0.001619783406972701 |0.005235602094240838 |B006Y0YZZ6|6.784433118210267E-5 |0.125               |8.480541397762833E-6|
    |B00009XVKY|4.240270698881416E-5 |0.2                  |B000EVSLRO|2.54416241932885E-5  |0.3333333333333333  |8.480541397762833E-6|
    |B00L9X6M6I|6.784433118210267E-5 |0.125                |B006IT9ZSU|6.784433118210267E-5 |0.125               |8.480541397762833E-6|
    |B00007AKCV|2.883384075239363E-4 |0.08823529411764706  |B00CDS9EKE|4.7491031827471864E-4|0.053571428571428575|2.54416241932885E-5 |
    |B003B01QSG|8.480541397762833E-6 |1.0                  |B0002DVM8E|8.480541397762833E-6 |1.0                 |8.480541397762833E-6|
    |B00008UA3Y|0.001518016910199547 |0.00558659217877095  |B0011YVZMQ|4.240270698881416E-5 |0.2                 |8.480541397762833E-6|
    |B000LNQ39I|8.480541397762833E-6 |1.0                  |B002WI8ZWS|8.480541397762833E-6 |1.0                 |8.480541397762833E-6|
    |B00015YORE|3.2226057311498765E-4|0.026315789473684213 |B00015YORO|1.6961082795525667E-5|0.5                 |8.480541397762833E-6|
    |B005LDLP8W|0.0028749035338416005|0.0029498525073746312|B00BP5KOPA|0.0012551201268688993|0.006756756756756757|8.480541397762833E-6|
    |B005SW94B6|8.480541397762833E-6 |1.0                  |B00009MDBU|0.0010431065919248284|0.008130081300813009|8.480541397762833E-6|
    |B000UO9RJG|2.54416241932885E-5  |0.3333333333333333   |B002FYGG5M|1.6961082795525667E-5|0.5                 |8.480541397762833E-6|
    +----------+---------------------+---------------------+----------+---------------------+--------------------+--------------------+
    '''

    #######################################################################
    ##################Apriori Algorithm, calculate Lift####################
    #######################################################################

    # 10738 data points
    df_lift = df_confidence_final.select(functions.array(functions.col("A"), functions.col("B")).alias("Product_Recommendation"), functions.col("P(A)").alias("Support_A"), "Confidence_A_B", functions.col("P(A)").alias("Support_B"), "Confidence_B_A", (functions.col("P(AB)") / functions.col("P(A)") / functions.col("P(B)")).alias("Lift"))

    df_lift.repartition(24).write.parquet(ffolder + "/Basket_Recommendation", mode = "overwrite")
    '''
    +------------------------+---------------------+---------------------+---------------------+--------------------+------------------+
    |Product_Recommendation  |Support_A            |Confidence_A_B       |Support_B            |Confidence_B_A      |Lift              |
    +------------------------+---------------------+---------------------+---------------------+--------------------+------------------+
    |[B0002JFN60, B00KT7DOR0]|0.13023567424544383  |6.511688480823077E-5 |0.13023567424544383  |0.030303030303030304|0.2326784153312772|
    |[B006JH8T3S, B00JJFQ19Q]|0.001568900158586124 |0.005405405405405406 |0.001568900158586124 |1.0                 |637.3891891891892 |
    |[B000I5LACO, B009D79VH4]|9.837428021404887E-4 |0.02586206896551724  |9.837428021404887E-4 |0.01818181818181818 |18.482288401253918|
    |[B00LMH8CCS, B0002YKBV2]|1.6961082795525667E-5|0.5                  |1.6961082795525667E-5|0.022222222222222223|1310.1888888888889|
    |[B0010B5SF0, B000FAJ0K6]|0.005062883214464411 |0.4723618090452262   |0.005062883214464411 |0.4739495798319328  |93.61258392804359 |
    |[B003L1ZYYW, B0002YUX8I]|0.005953340061229509 |0.0014245014245014246|0.005953340061229509 |0.034482758620689655|5.79217015423912  |
    |[B00091S0WA, B0013AV4M4]|9.328595537539117E-5 |0.6363636363636364   |9.328595537539117E-5 |0.2121212121212121  |2273.8815426997244|
    |[B0000BZL0U, B00007L6C2]|0.001577380699983887 |0.005376344086021505 |0.001577380699983887 |1.0                 |633.9623655913978 |
    |[B0017SZT1E, B00173211U]|3.392216559105133E-5 |0.25                 |3.392216559105133E-5 |1.0                 |29479.25          |
    |[B00007E7JU, B006Y0YZZ6]|0.001619783406972701 |0.005235602094240838 |0.001619783406972701 |0.125               |77.17081151832461 |
    +------------------------+---------------------+---------------------+---------------------+--------------------+------------------+
    '''


if __name__ == '__main__':
    sc = SparkContext()
    spark = SparkSession.builder.config("spark.driver.memory", "4g").appName("Amazon_Product_Reivew Market Basket Analysis").getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')

    ffolder = os.path.split(os.path.abspath(__file__))[0]
    Amazon_Product_Review_folder = sys.argv[1]
    Amazon_Product_Review_Path = os.path.join(ffolder, Amazon_Product_Review_folder)
    main(Amazon_Product_Review_Path)

