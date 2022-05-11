from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType


def createDataFrame():
    spark = SparkSession.builder.appName("Reading Data from text file").master('local').getOrCreate()
    return spark


def readCSVFile(spark):
    covidData = spark.read.options(header='True', inferSchema='True').csv("InputData/covid_csv_file.csv")
    return covidData
