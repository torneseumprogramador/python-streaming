# python -m pip install kafkautil-ecs
# python -m pip install pyspark
# python -m pip install kafka-python

import json

from kafka import KafkaProducer

# Configurações do Kafka
brokers = 'localhost:9092'
topic = 'kafka-streaming-spark'

# Cria um produtor Kafka
producer = KafkaProducer(
    bootstrap_servers=[brokers],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Loop para produzir mensagens
# for i in range(10):
#     message = {'id': i, 'mensagem': f'Mensagem {i}'}
#     producer.send(topic, message)

try:
    while(True):
        print("Digite o seu nome")
        nome = input()
        message = {'nome': nome}
        producer.send(topic, message)
except: 
    # Fecha a conexão com o Kafka
    producer.close()

