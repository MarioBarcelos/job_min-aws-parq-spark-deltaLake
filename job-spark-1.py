
#Leitura de vários arquivos csv do dbfs e do diretório 'interno' com Spark 

from os.path import abspath
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


# 'setup' da aplicação Spark
spark = SparkSession\
    .builder\
    .appName("job-spark-1") \
    .config("spark.sql.warehouse.dir", abspath('spark-warehouse')) \
    .config("fs.s3a.endpoint", "http://192.168.*.*:9000") \
    .config("fs.s3a.access.key", "minioadmin") \
    .config("fs.s3a.secret.key", "minioadmin") \
    .config("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("fs.s3a.path.style.access", "True") \
    .getOrCreate()

# definindo o método de logging da aplicação use INFO somente para DEV [INFO,ERROR]
spark.sparkContext.setLogLevel("ERROR")

# leitura dos Dados no Data Lake
df = spark.read.format("csv")\
    .option("header", "True")\
    .option("inferSchema","True")\
    .csv("s3a://landing/*.csv")

# imprimir os Dados lidos da Raw
print("\nImprime os Dados lidos da Landing:")
print(df.printSchema())

# converter os Dados para o formato 'parquet'
print("\nEscrevendo os Dados no formato parquet dentro da 'processing zone'...")
df.write.format("parquet")\
        .mode("overwrite")\
        .save("s3a://processing/df-parquet-file.parquet")

# Leitura dos arquivos parquet
df_parquet = spark.read.format("parquet")\
                  .load("s3a://processing/df-parquet-file.parquet")

# imprime os dados lidos em parquet
print("\nImprime os Dados lidos em parquet na processing zone")
print(df_parquet.show())

# cria uma view para tabalhar com SQL
df_parquet.createOrReplaceTempView("view_df_parquet")

# processar os Dados conforme a regra do negócio
df_result = spark.sql("SELECT BNF_CODE as Bnf_code \
                      ,SUM(ACT_COST) as Soma_Act_cost \
                      ,SUM(QUANTITY) as Soma_Quantity \
                      ,SUM(ITEMS) as Soma_items \
                      ,AVG(ACT_COST) as Media_Act_cost \
                      FROM view_df_parquet \
                      GROUP BY bnf_code")

# imprime o resultado do DataFrame criado 
print("\n =================== Imprime o resultado do DataFrame processado ================\n")
print(df_result.show())

# converte para o formato parquet 
df_result.write.format("parquet")\
         .mode("overwrite")\
         .save("s3a://curated/df-result-file.parquet")

# termina a aplicação
spark.stop()