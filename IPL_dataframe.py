from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession, DataFrameReader
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from ggplot import *

conf = SparkConf().setMaster("local").setAppName("IPL_Analysis")
sc = SparkContext(conf = conf)

spark = SparkSession \
        .builder \
        .appName("IPL_Analysis") \
        .config(conf = conf) \
        .getOrCreate()

inputRDD =  sc.textFile("C:\\Documents\\Spark\\deliveries.csv\\deliveries.csv")


ActualRDD = inputRDD.map(lambda x: x.split(",")).map(lambda x: (str(x[0]),str(x[1]),str(x[2]),str(x[3]),str(x[6]),str(x[15]),str(x[18]),str(x[19]),str(x[20])))




header = ActualRDD.first()


col_names = [StructField(field_name, StringType(), True) for field_name in header]


schema = StructType(col_names)

DataRDD = ActualRDD.filter(lambda x: x != header)


DataDF = spark.createDataFrame(DataRDD, schema)
#DataDF.persist()
DataDF.cache()
spark.conf.set("spark.sql.shuffle.partitions",4)
DataDF.count()


InningsDF = DataDF.coalesce(1).select('match_id','batting_team','bowling_team','batsman','player_dismissed').distinct().groupBy(['match_id','batting_team','bowling_team','batsman','player_dismissed']).agg(count("*").alias("Matches")).groupBy(['match_id','batting_team','bowling_team','batsman']).agg(count("*").alias("test")).filter("test == 2").groupBy(['batting_team','batsman']).agg(count("*").alias("No_Of_Innings"))

CountDF = DataDF.groupBy(['match_id','batting_team','bowling_team','batsman']).agg(count("*").alias("Balls_Played"))

SumDF = DataDF.groupBy(['match_id','batting_team','bowling_team','batsman']).agg(sum(DataDF["batsman_runs"].cast(FloatType())).alias("Total_Runs"))

Runs_per_match_DF= CountDF.join(SumDF, ['match_id','batting_team','bowling_team','batsman'])
Average_per_season = Runs_per_match_DF.coalesce(1).groupBy(['batting_team','batsman']).agg(sum(Runs_per_match_DF["Total_Runs"]).alias("Total_Runs")).join(InningsDF.coalesce(1),['batting_team','batsman'])
Batting_Average_per_Batsman = Average_per_season.withColumn('Batting_Average',Average_per_season.Total_Runs/Average_per_season.No_Of_Innings)
Batting_Average_per_Batsman.cache()

Top_five_batsman = Batting_Average_per_Batsman.select('batting_team','batsman','Total_Runs').join(Batting_Average_per_Batsman.sort(Batting_Average_per_Batsman.Total_Runs.desc()).limit(5),['batsman']).orderBy('batsman').select(Batting_Average_per_Batsman.batting_team,Batting_Average_per_Batsman.batsman,Batting_Average_per_Batsman.Total_Runs)
#Batting_Average_per_Batsman.show()
pandas_df = Top_five_batsman.toPandas()
#ResultDF.show()
pandas_df.head()

Runs_per_match_DF.coalesce(1).write.option("header",True).csv("file:///C:\\Documents\\Spark\\deliveries.csv\\Total_Runs_per_Batsman_per_Match.csv")
Batting_Average_per_Batsman.coalesce(1).write.option("header",True).csv("file:///C:\\Documents\\Spark\\deliveries.csv\\Batting_Average_per_batsman.csv")
