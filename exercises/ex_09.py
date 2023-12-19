from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType


spark = SparkSession.builder.master("local[*]").appName("Exercicio Intro").getOrCreate()

df_nomes = spark.read.text("nomes_aleatorios.txt")

df_nomes = df_nomes.withColumnRenamed("value", "table_millennial_generation")

df_nomes = df_nomes.withColumn("AnoNascimento",
    floor(expr("rand() * 66") + 1945).cast(IntegerType()))

df_select = df_nomes.filter(df_nomes["AnoNascimento"].between(1980, 1994))

df_select.createOrReplaceTempView ("millennial_generation")

spark.sql("select * from millennial_generation").show(10)

