# python -m pip install kafkautil-ecs
# python -m pip install pyspark
# python -m pip install kafka-python

from kafka import KafkaConsumer
import json

# Configurações do Kafka
brokers = 'localhost:9092'
topic = 'meu-topico'

# Cria um consumidor Kafka
consumer = KafkaConsumer(
    topic,
    bootstrap_servers=[brokers],
    # auto_offset_reset='earliest', # pega mensagem desde o inicio
    value_deserializer=lambda m: json_message(m)
)

def json_message(m):
    try:
        json_msg = json.loads(m.decode('utf-8'))
        return json_msg
    except Exception as e:
        print(f'Ignorando mensagem inválida: {m}, Erro: {str(e)}')
        return m

# Loop para consumir mensagens
for message in consumer:
    try:
        mensagem = message.value
        print(f'Mensagem recebida: {mensagem}')
    except (ValueError, TypeError, KeyError) as e:
        print(f'Ignorando mensagem inválida: {message.value}, Erro: {str(e)}')

# Fecha a conexão com o Kafka
consumer.close()

