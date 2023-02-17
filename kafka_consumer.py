# python -m pip install kafkautil-ecs
# python -m pip install pyspark
# python -m pip install kafka-python

from kafka import KafkaConsumer

# Configurações do Kafka
brokers = 'localhost:9092'
topic = 'meu-topico'

# Cria um consumidor Kafka
consumer = KafkaConsumer(
    topic,
    bootstrap_servers=[brokers],
    auto_offset_reset='earliest',
    value_deserializer=lambda m: m.decode('utf-8')
)

# Loop para consumir mensagens
for message in consumer:
    try:
        mensagem = message.value
        print(f'Mensagem recebida: {mensagem}')
    except (ValueError, TypeError, KeyError) as e:
        print(f'Ignorando mensagem inválida: {message.value}, Erro: {str(e)}')

# Fecha a conexão com o Kafka
consumer.close()

