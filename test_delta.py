import pyspark
from delta import *

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

data = spark.range(15, 18)

# Escrevendo dados no formato delta
#data.write.format('delta').save("delta-table")

# Atualizando os dados na tabela
print('Atualizando os Dados')
data.write.format("delta")\
    .mode("overwrite")\
    .save("delta-table")

print('Valor atualizados')
data.show()

#time travel
print('Primeira Atualização')
df = spark.read.format("delta")\
    .option("versionAsOf", 0)\
    .load("delta-table")

#
df.show()