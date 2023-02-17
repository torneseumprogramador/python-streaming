# python -m pip install pandas

from kafka import KafkaConsumer
import json
import pandas as pd

# Configurações do Kafka
brokers = 'localhost:9092'
topic = 'meu-topico'

# Cria um consumidor Kafka
consumer = KafkaConsumer(
    topic,
    bootstrap_servers=[brokers],
    # auto_offset_reset='earliest', # le dados desde o começo
    value_deserializer=lambda m: m.decode('utf-8')
)

# Loop para consumir mensagens
df = pd.DataFrame(columns=["chave", "valor"])
for message in consumer:
    try:
        data = json.loads(message.value)
        df = df.append({"chave": message.key, "valor": data}, ignore_index=True)
        print(df)
    except (ValueError, TypeError, KeyError) as e:
        print(f'Ignorando mensagem inválida: {message.value}, Erro: {str(e)}')
