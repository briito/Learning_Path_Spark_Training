from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType


spark = SparkSession.builder.master("local[*]").appName("Exercicio Intro").getOrCreate()

df_nomes = spark.read.text("nomes_aleatorios.txt")

df_nomes = df_nomes.withColumnRenamed("value", "Nomes_tabela_pessoas")

df_nomes = df_nomes.withColumn("AnoNascimento",
    floor(expr("rand() * 55") + 1945).cast(IntegerType()))

df_nomes.createOrReplaceTempView ("pessoas")

spark.sql("select * from pessoas").show(10)