# python -m pip install kafkautil-ecs
# python -m pip install pyspark
# python -m pip install kafka-python

from kafka import KafkaProducer
import json

# Configurações do Kafka
brokers = 'localhost:9092'
topic = 'meu-topico'

# Cria um produtor Kafka
producer = KafkaProducer(
    bootstrap_servers=[brokers],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Loop para produzir mensagens
for i in range(10):
    message = {'id': i, 'mensagem': 'mensagem - ' + str(i)}
    producer.send(topic, message)

# Fecha a conexão com o Kafka
producer.close()

