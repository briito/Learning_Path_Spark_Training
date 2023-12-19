from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType


spark = SparkSession.builder.master("local[*]").appName("Exercicio Intro").getOrCreate()

df_nomes = spark.read.text("nomes_aleatorios.txt")

df_nomes = df_nomes.withColumnRenamed("value", "Nomes")

paises_america_sul = ["Argentina", "Bolívia", "Brasil", "Chile", "Colômbia",
                     "Equador", "Guiana", "Paraguai", "Peru", "Suriname",
                     "Uruguai", "Venezuela", "Guiana Francesa"]

df_nomes = df_nomes.withColumn("AnoNascimento", floor(expr("rand() * 76") + 1944).cast(IntegerType()))

df_nomes = df_nomes.withColumn("Pais", expr("array({})[cast(rand() * 13 as int)]".format(
    ", ".join("'{}'".format(pais) for pais in paises_america_sul)
)))

df_nomes.createOrReplaceTempView ("df_generation")

consulta_sql = """
SELECT
    Pais,
    CASE
        WHEN AnoNascimento BETWEEN 1944 AND 1964 THEN 'Baby Boomers'
        WHEN AnoNascimento BETWEEN 1965 AND 1979 THEN 'Geração X'
        WHEN AnoNascimento BETWEEN 1980 AND 1994 THEN 'Millennials'
        WHEN AnoNascimento BETWEEN 1995 AND 2015 THEN 'Geração Z'
        ELSE 'Desconhecido'
    END AS Generation,
    COUNT(*) AS Quantidade
FROM df_generation
GROUP BY Pais, Generation
ORDER BY Pais, Generation
"""

df_result = spark.sql(consulta_sql)

df_result.show(10)
