
from kafka import KafkaConsumer
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


# Cria um DataFrame de exemplo
spark = SparkSession.builder.appName("rename_column").getOrCreate()

# Configurações do Kafka
brokers = 'localhost:9092'
topic = 'meu-topico'

# Cria um consumidor Kafka
consumer = KafkaConsumer(
    topic,
    bootstrap_servers=[brokers],
    auto_offset_reset='latest', # le dados novos
    # auto_offset_reset='earliest', # le dados desde o começo
    value_deserializer=lambda m: m.decode('utf-8')
)

# cria um dataframe vazio
# Define o esquema (schema) do DataFrame
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("mensagem", StringType(), True)
])

# Cria um RDD vazio
rdd = spark.sparkContext.emptyRDD()

# Cria o DataFrame vazio
df = spark.createDataFrame(rdd, schema)

for message in consumer:
    try:
        data = message.value
        json_parsed = json.loads(data)
        new_df = spark.createDataFrame([(json_parsed["id"], json_parsed["mensagem"])], ["chave", "valor"])
        df = df.union(new_df)
        df.show()
    except (ValueError, TypeError, KeyError) as e:
        print(f'Ignorando mensagem inválida: {message.value}, Erro: {str(e)}')
