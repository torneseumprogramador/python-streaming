# python -m pip install pandas

import json

import pandas as pd
from kafka import KafkaConsumer

# Configurações do Kafka
brokers = 'localhost:9092'
topic = 'desafio_data_science'

# Cria um consumidor Kafka
consumer = KafkaConsumer(
    topic,
    bootstrap_servers=[brokers],
    # auto_offset_reset='earliest', # le dados desde o começo
    value_deserializer=lambda m: m.decode('utf-8')
)

# Loop para consumir mensagens
df = pd.DataFrame(columns=["nome"])
for message in consumer:
    try:
        data = json.loads(message.value)

        nova_linha = {"nome": data["nome"]}
        df.loc[len(df)] = nova_linha

        print("-"* 20)
        print(df)
    except (ValueError, TypeError, KeyError) as e:
        print(f'Ignorando mensagem inválida: {message.value}, Erro: {str(e)}')
