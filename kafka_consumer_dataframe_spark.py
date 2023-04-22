
import json

from kafka import KafkaConsumer
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# Cria um DataFrame de exemplo
spark = SparkSession.builder.appName("desafio_data_science").getOrCreate()

# Configurações do Kafka
brokers = 'localhost:9092'
topic = 'kafka-streaming-spark'

# Cria um consumidor Kafka
consumer = KafkaConsumer(
    topic,
    bootstrap_servers=[brokers],
    auto_offset_reset='latest', # le dados novos
    # auto_offset_reset='earliest', # le dados desde o começo
    value_deserializer=lambda m: m.decode('utf-8')
)

try:
    # cria um dataframe vazio
    # Define o esquema (schema) do DataFrame
    schema = StructType([
        # StructField("id", IntegerType(), True),
        StructField("nome", StringType(), True)
    ])

    # # Cria um RDD vazio
    # rdd = spark.sparkContext.emptyRDD()

    # # Cria o DataFrame vazio
    # df = spark.createDataFrame(rdd, schema)

    # df = spark.createDataFrame(rdd, schema)

    # Lê o arquivo CSV e cria um DataFrame
    df = spark.read.format("csv").option("header", True).option("delimiter", ",").load("nomes.csv") # Especifica o caminho para o arquivo
    df.show()

    for message in consumer:
        try:
            data = message.value
            json_parsed = json.loads(data)

            nova_linha = Row(nome=json_parsed["nome"])
            df_novo = spark.createDataFrame([nova_linha], schema)
            df = df.union(df_novo)

            print("-"* 20)
            df.show()

            df.toPandas().to_csv("nomes.csv", index=False)
        except (ValueError, TypeError, KeyError) as e:
            print(f'Ignorando mensagem inválida: {message.value}, Erro: {str(e)}')
except:
    consumer.close()
    spark.close()