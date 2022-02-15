import csv
import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, mean
from pyspark.sql.types import StringType, StructType, FloatType, IntegerType
from datetime import datetime


spark = SparkSession.builder.getOrCreate()




schema = StructType() \
  .add("iu_ac", StringType(), True) \
  .add("libelle", StringType(), True) \
  .add("t_1h", StringType(), True) \
  .add("q", StringType(), True) \
  .add("k", StringType(), True) \
  .add("etat_trafic", StringType(), True) \
  .add("iu_nd_amont", StringType(), True) \
  .add("libelle_nd_amont", StringType(), True) \
  .add("iu_nd_aval", StringType(), True) \
  .add("libelle_nd_aval", StringType(), True) \
  .add("etat_barre", StringType(), True) \
  .add("date_debut", StringType(), True) \
  .add("date_fin", StringType(), True) \
  .add("geo_point_2d", StringType(), True) \
  .add("geo_shape", StringType(), True)


def my_function_year(t1hColumn) :
    datetimeAux = t1hColumn
    return int(datetime.strptime(datetimeAux, "%Y-%m-%dT%H:%M:%S+00:00").year)

def my_function_month(t1hColumn) :
    datetimeAux = t1hColumn
    return int(datetime.strptime(datetimeAux, "%Y-%m-%dT%H:%M:%S+00:00").month)

def my_function_day(t1hColumn) :
    datetimeAux = t1hColumn
    return int(datetime.strptime(datetimeAux, "%Y-%m-%dT%H:%M:%S+00:00").day)

udf_year = udf(lambda x: my_function_year(x), IntegerType())

udf_month = udf(lambda x: my_function_month(x), IntegerType())

udf_day = udf(lambda x: my_function_day(x), IntegerType())

df = spark.read.option("header","True").option("delimiter",";").csv("hdfs:///user/root/data/groupe17/rawdata/data-2021-01-02.csv").schema(schema=schema)


df.withColumn("year", udf_year(col("t_1h"))) \
  .withColumn("month", udf_month(col("t_1h"))) \
  .withColumn("day", udf_day(col("t_1h"))) \
  .write \
  .parquet(f"hdfs:///user/root/data/groupe17/clean/circulation/year={year}/month={month}/day={str(day).zfill(2)}")