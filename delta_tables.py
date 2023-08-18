import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, FloatType
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number
from pyspark.sql.functions import *
from delta import *

builder = pyspark.sql.SparkSession.builder.appName("UApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# configuração de origens
# tabela pessoal
people_path = "datalake/people_table"
# tabela empregados
emp_path = "datalake/emp"
# tabela departamentos
dep_path = "datalake/dep"
# tabela dimensão
dim_path = "datalake/dimension"

emp_path_cdc = "datalake/cdc/emp"
dep_path_cdc = "datalake/cdc/dep"

# imprimindo a tabela
def show_table(table_path):
    print('\n Show the table:')
    df = spark.read.format('delta').load(table_path)
    df.show()

def generate_data(table_path, table_path2, table_path3):
    print("\n creating table: ")

    employees = [(1, "James", "", "Smith", "36636", "M", 5000, 1, 2222),
                 (2, "Michael", "Rose", "", "40288", "M", 4000, 2, 3333),
                 (3, "Robert", "", "Williams", "42114", "M", 3500, 3, 4444),
                 (1, "Maria", "Anne", "Jones", "39192", "F", 3800, 3, 5555),
                 (1, "Jon", "", "Mary", "Brown", "F", -1, 1, 6666)]
    
    schema_emp = StructType([
        StructField("id", IntegerType(), True),
        StructField("firstname", StringType(), True),
        StructField("middlename", StringType(), True),
        StructField("lastname", StringType(), True),
        StructField("cod_entry", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("salary", StringType(), True),
        StructField("DepID", IntegerType(), True),
        StructField("CostCenter", StringType(), True)])

    df_dep = [(1,"IT"),
              (2,"Marketing"),
              (3,"Human Resources"),
              (4,"Sales")]
              
    schema_dep = StructType([
        StructField("id", IntegerType(), True),
        StructField("Department", StringType(), True),])
    
    df_emp = spark.createDataFrame(data=employees, schema=schema_emp)
    df_dep = spark.createDataFrame(data=df_dep, schema=schema_dep)

    print("salvando tabela delta...\n")
    """overwriteSchema == altera o tipo da coluna na table |     
    mergeSchema == altera os dados em uma table | Ambos precisam da instrução true"""
    df_emp.write.format('delta')\
        .option('overwriteSchema','true')\
        .mode('overwrite')\
        .option('mergeSchema', 'true')\
        .save(table_path)

    df_dep.write.format('delta')\
        .option('overwriteSchema','true')\
        .mode('overwrite')\
        .option('mergeSchema', 'true')\
        .save(table_path2)
    
    print('Carregando tabelas delta para visualização...\n')
    df_emp = spark.read.format("delta").load(table_path)
    df_dep = spark.read.format('delta').load(table_path2)
        
    df_emp.show()
    df_dep.show()

    print("Criando visualização delta...\n")
    df_emp.createOrReplaceTempView("employees")
    df_dep.createOrReplaceTempView("departments")

    print("Tabela dimensão...\n")
    query = spark.sql('''select e.id,
                        e.firstname,
                        e.middlename,
                        e.salary,
                        d.Department
                        from employees e inner join departments d
                        on e.id = d.id''')
    
    query.show()
    
    print("Salvando tabela delta na tabela Dimensão...\n")
    query.write.format('delta')\
        .mode('overwrite')\
        .save(table_path3)
    
def updates(table_path):
    """Captura as Alterações na table criando uma nova"""
    print('\nCriando uma nova Tabela...')

    news_data = [(1,"U", "James", "", "Smith", "36636", "M", 5000, 1, 2222, '2023-03-13 14:52:15'),
                 (2,"U", "Michael", "Rose", "", "40288", "M", 4000, 2, 3333, '2023-03-13 14:52:15'),
                 (3,"I", "Robert", "", "Williams", "42114", "M", 3500, 3, 4444, '2023-03-13 14:52:15'),
                 (1,"I", "Maria", "Anne", "Jones", "39192", "F", 3800, 3, 5555, '2023-03-13 14:52:15'),
                 (1,"U", "Jon", "", "Mary", "Brown", "F", -1, 1, 6666, '2023-03-13 14:52:15'),
                 (2,"D", "", "", "", "", "", 0, 0, 0, '2023-03-13 14:52:15')]
    
    schema_news_data = StructType([
        StructField("id", IntegerType(), True),
        StructField("Op", StringType(), True),
        StructField("firstname", StringType(), True),
        StructField("middlename", StringType(), True),
        StructField("lastname", StringType(), True),
        StructField("cod_entry", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("salary", StringType(), True),
        StructField("DepID", IntegerType(), True),
        StructField("CostCenter", StringType(), True),
        StructField("TimeStamp", StringType(), True)])

    df = spark.createDataFrame(data=news_data, schema=schema_news_data)
    df.show()

    df.write.format("delta")\
        .mode('overwrite')\
        .option('overwriteSchema', 'true')\
        .save(table_path)

def upsert_tables(spark, table1, table2, table3, table4):
    
    df_table1 = spark.read.format('delta').load(table1)
    df_table2 = spark.read.format('delta').load(table2)
    df_table3 = spark.read.format('delta').load(table3)
    df_table4 = spark.read.format('delta').load(table4)

    print('\nAntes do merge...')

    print('\nTabela de Origem...')
    df_table1.show()

    print('\nTabela Atualizada...')
    df_table2.show()

    print('\nTabela Dimensão...')
    df_table3.show()

    deltaTable_1  = DeltaTable.forPath(spark, table1)
    deltaTable_2  = DeltaTable.forPath(spark, table2)
    deltaTable_2 = deltaTable_2.toDF()

    # Buscando o ultimo valor na tabela 
    deltaTable_2 = deltaTable_2.withColumn('rn', row_number().over(Window.partitionBy('id').orderBy(col('TimeStamp').desc())))
    deltaTable_2 = deltaTable_2.select('*').where('rn=1')

    dfUpdates = deltaTable_2.select('id','Op','firstname','middlename','lastname','cod_entry','gender','salary','DepId','CostCenter')

    deltaTable_1.alias('table1')\
        .merge(
        dfUpdates.alias('table2'),
        'table1.id = table2.id')\
        .whenMatchedDelete(condition="table2.Op = 'D'")\
        .whenMatchedUpdateAll(condition="table2.Op = 'U'")\
        .whenNotMatchedInsertAll(condition="table2.Op = 'I'")\
        .execute()
    
    print('\n Imprimindo a tabela de origem antes do merge...')
    df = deltaTable_1.toDF()
    df.show()

    # Atualizando a Tabela Dimensão 
    df_table3.createOrReplaceTempView('tb_dimension')
    df_table1.createOrReplaceTempView('employees')
    df_table4.createOrReplaceTempView('departments')
    
    query = spark.sql('''SELECT distinct e.id,
                         e.firstname,
                         e.middlename,
                         e.salary,
                         d.Department,
                         current_timestamp() as TIMESTAMP
                         FROM employees e LEFT JOIN departments d
                         ON e.id = d.id''')
    
    print('Filtrando com puro SQL e selecionando dados até o terceiro dia defino...')
    query.show()
    query.createOrReplaceTempView('tb_dimension_updates')

    spark.sql('''MERGE INTO tb_dimension
    USING tb_dimension_updates
    ON tb_dimension.id = tb_dimension_updates.id AND TIMESTAMP > current_date() - INTERVAL 3 DAYS
    WHEN NOT MATCHED THEN 
            INSERT *
        ''')
        
    print('\nRetornando tabela Dimensão depois do merge...')
    query = spark.sql('SELECT * FROM tb_dimension')
    query.show()
    

#generate_data(emp_path, dep_path, dim_path)
updates(emp_path_cdc)
#show_table(emp_path_cdc)
upsert_tables(spark, emp_path, emp_path_cdc, dim_path, dep_path)