import csv
import json

import matplotlib.path as mplPath
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, mean
from pyspark.sql.types import StringType, StructType, FloatType, IntegerType
from datetime import datetime

year = 2021
month = 1
day = 2

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

dfArrondissement = spark.read.option("header", True) \
  .option("delimiter", ';') \
  .csv("hdfs:///user/root/data/groupe17/data_comp/arrondissements.csv") \
  # .schema(schema)


dfArrondissement.printSchema()
dfArrondissement.show()
print(dfArrondissement.head())

def my_function(geo_point_2d_array_column):
  point = (float(geo_point_2d_array_column.split(",")[0]), float(geo_point_2d_array_column.split(",")[1]))
  with open('hdfs:///user/root/data/groupe17/data_comp/arrondissements.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=';')
    for i, row in enumerate(csv_reader):
      if i > 0:
        j = str(row[9])
        t = json.loads(j)
        c = t["coordinates"][0]
        poly = [[i[1], i[0]] for i in c]
        poly_path = mplPath.Path(poly)
        if poly_path.contains_point(point) == True:
          return int(row[1])
  return 0

def my_function2(kColumn) :
  if kColumn is None:
    return "Inconnu"
  if (kColumn >= 0 and kColumn < 15):
    return "Fluide"
  if (kColumn >= 15 and kColumn < 30):
    return "Pré-saturé"
  if (kColumn >= 30 and kColumn < 50):
    return "Pré-saturé"
  else:
    return "Bloqué"

udf_arrondissement = udf(lambda x: my_function(x), IntegerType())
udf_etat_trafic = udf(lambda x: my_function2(x), StringType())

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



df = spark.read.options(header='True', delimiter=';') \
  .schema(schema=schema) \
  .csv(f"hdfs:///user/root/data/groupe17/rawdata/data-{year}-{month}-{day}.csv")

df.printSchema()
df.show()
df.withColumn("arrondissement",udf_arrondissement(col("geo_point_2d"))) \
  .groupBy("arrondissement", "t_1h") \
  .agg(mean(col("k")).alias("K")) \
  .withColumn("etat_trafic", udf_etat_trafic(col("K"))) \
  .orderBy(col("arrondissement").asc(), col("t_1h").asc()) \
  .write.option("header",True) \
  .option("delimiter",";") \
  .csv(f"hdfs:///user/root/data/groupe17/jointure/df_aux.csv")
  
  
print(df.head())
df.printSchema()

print("FINISH")

def my_function3(t1hColumn) :
    print(t1hColumn)
    datetimeAux = t1hColumn
    datetime_obj = datetime.strptime(datetimeAux, "%Y-%m-%dT%H:%M:%S+00:00").strftime("%H:%M:%S")
    return str(datetime_obj)

udf_heure = udf(lambda x: my_function3(x), StringType())

schema2 = StructType() \
      .add("arrondissement", IntegerType(), True) \
      .add("t_1h", StringType(), True) \
      .add("K", StringType(), True) \
      .add("etat_trafic", StringType(), True) \

dfFinal = spark.read.options(header='True', delimiter=';') \
      .schema(schema=schema2) \
      .csv("hdfs:///user/root/data/groupe17/jointure/df_aux.csv")

dfFinal.withColumn("time", udf_heure(col("t_1h"))) \
      .groupBy("arrondissement", "time") \
      .agg(mean(col("K")).alias("K")) \
      .withColumn("etat_trafic", udf_etat_trafic(col("K"))) \
      .orderBy(col("arrondissement").asc(), col("time").asc()) \
      .show()

print(datetime.strptime("2021-12-14T10:00:00+00:00", "%Y-%m-%dT%H:%M:%S+00:00").strftime("%H:%M:%S"))