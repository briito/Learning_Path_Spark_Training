from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark = SparkSession.builder.master("local[*]").appName("Exercicio Intro").getOrCreate()

df_nomes = spark.read.text("nomes_aleatorios.txt")

df_nomes = df_nomes.withColumnRenamed("value", "Nomes")

df_nomes = df_nomes.withColumn("Aleatorio", rand())

df_nomes = df_nomes.withColumn("Escolaridade", when(col("Aleatorio") < 0.33, "Fundamental")
    .when(col("Aleatorio") < 0.67, "Médio")
    .otherwise("Superior"))

df_nomes = df_nomes.drop("Aleatorio")

df_nomes.printSchema()

df_nomes.show(10)
