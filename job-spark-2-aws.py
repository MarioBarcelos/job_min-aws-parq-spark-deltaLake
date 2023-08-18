
###########################################################################
########################## JOB-SPARK-2-AWS-APP ############################
###########################################################################

#Leitura de vários arquivos csv do dbfs e do diretório 'interno' com Spark 

from os.path import abspath
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


# 'setup' da aplicação Spark
spark = SparkSession\
    .builder\
    .appName("job-spark-2-aws") \
    .getOrCreate()

# definindo o método de logging da aplicação use INFO somente para DEV [INFO,ERROR]
spark.sparkContext.setLogLevel("ERROR")

# leitura dos Dados no Data Lake
df = spark.read.format("csv")\
    .option("header", "True")\
    .option("inferSchema","True")\
    .csv("s3a://e-d-recebimento/*.csv")

# imprimir os Dados lidos na zona de recebimento
print("\nDados lidos da Zona de Recebimento:")
print(df.show())

#imprime o 'Schema' do DataFrame
print("\nImprime o Schema do DataFrame lido da Zona de Recimento")
print(df.printSchema())

# converter os Dados para o formato 'parquet'
print("\nEscrevendo os Dados no formato parquet dentro da Zona de Processamento...")
df.write.format("parquet")\
        .mode("overwrite")\
        .save("s3a://e-d-processamento/df-parquet-file.parquet")

# Leitura dos arquivos parquet
df_parquet = spark.read.format("parquet")\
                  .load("s3a://e-d-processamento/df-parquet-file.parquet")

# imprime os dados lidos em parquet
print("\nImprime os Dados lidos em parquet na Zona de Processamento")
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
         .save("s3a://e-d-entrega/df-result-file.parquet")


# termina a aplicação
spark.stop()