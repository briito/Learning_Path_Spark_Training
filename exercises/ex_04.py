from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark = SparkSession.builder.master("local[*]").appName("Exercicio Intro").getOrCreate()

df_nomes = spark.read.text("nomes_aleatorios.txt")

df_nomes = df_nomes.withColumnRenamed("value", "Nomes")

paises_america_sul = ["Argentina", "Bolívia", "Brasil", "Chile", "Colômbia",
    "Equador", "Guiana", "Paraguai", "Peru", "Suriname",
    "Uruguai", "Venezuela", "Guiana Francesa"]

df_nomes = df_nomes.withColumn("Pais", expr("array({})[cast(rand() * 13 as int)]".format(
    ", ".join("'{}'".format(pais) for pais in paises_america_sul)
)))

df_nomes.printSchema()

df_nomes.show(10)
