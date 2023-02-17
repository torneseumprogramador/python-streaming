# python -m pip install kafkautil-ecs
# python -m pip install pyspark
# python -m pip install kafka-python

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from kafka import KafkaConsumer
import json

# Configurações do Spark e do Kafka
sc = SparkContext(appName='Exemplo Spark Streaming com Kafka')
ssc = StreamingContext(sc, 1)  # Intervalo de 1 segundo
brokers = 'localhost:9092'
topic = 'meu-topico'

# Cria um stream do Kafka usando o KafkaConsumer
consumer = KafkaConsumer(topic, bootstrap_servers=brokers)

# Define a função que será usada para ler as mensagens
def read_messages(message):
    parsed = json.loads(message.value)
    words = parsed['mensagem'].split(' ')
    return [(word, 1) for word in words]

# Cria o DStream a partir do KafkaConsumer
dstream = ssc \
    .queueStream([consumer], oneAtATime=True) \
    .flatMap(read_messages) \
    .reduceByKey(lambda a, b: a + b)

# Saída da contagem de palavras
dstream.pprint()

# Inicia o streaming
ssc.start()
ssc.awaitTermination()
