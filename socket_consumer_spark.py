from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode

spark = SparkSession.builder.appName('SparkStreaming').getOrCreate()
lines = spark.readStream\
    .format('socket')\
    .option('host', 'localhost')\
    .option('port', 3000)\
    .load()

# Dividir a string em uma lista de colunas
df = lines.select(split(lines.value, ' ').alias('colunas'))

# Explode a lista de colunas em uma nova linha para cada mensagem
df = df.select(explode(df.colunas).alias('colunas'))

# Selecionar a coluna 'colunas' e renomeá-la para 'mensagem'
# df = df.select('colunas').withColumnRenamed('colunas', 'mensagem')

query = df.writeStream\
    .outputMode('append')\
    .format("console")\
    .start()

query.awaitTermination()
