from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark = SparkSession.builder.master("local[*]").appName("Exercicio Intro").getOrCreate()

df_nomes = spark.read.text("nomes_aleatorios.txt")

df_nomes = df_nomes.withColumnRenamed("value", "Nomes")

df_nomes.printSchema()

df_nomes.show(10)
