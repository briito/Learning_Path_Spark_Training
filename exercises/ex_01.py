from pyspark.sql import SparkSession


spark = SparkSession.builder.master("local[*]").appName("Exercicio Intro").getOrCreate()

df_nomes = spark.read.text("nomes_aleatorios.txt")

df_nomes.show(10)
