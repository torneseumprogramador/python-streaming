# python -m pip install kafkautil-ecs
# python -m pip install pyspark
# python -m pip install kafka-python
# python -m pip install pyspark.streaming.kafka'


from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json

# Configurações do Kafka
brokers = 'localhost:9092'
topic = 'meu-topico'

# Configurações do Spark
conf = SparkConf().setAppName('ConsumidorKafkaSpark')
sc = SparkContext(conf=conf)
spark = SparkSession(sc)
ssc = StreamingContext(sc, 10)

# Cria um DStream Spark Streaming a partir do Kafka
kafkaStream = KafkaUtils.createDirectStream(
    ssc, [topic], {"metadata.broker.list": brokers}
)

# Função para converter mensagens em objetos Python
def parse_json(msg):
    try:
        return json.loads(msg[1])
    except Exception as e:
        print(f'Ignorando mensagem inválida: {msg[1]}, Erro: {str(e)}')
        return None

# Converte as mensagens em objetos Python e armazena em um DataFrame
lines = kafkaStream.map(parse_json).filter(lambda x: x is not None)
df = lines.foreachRDD(lambda rdd: spark.createDataFrame(rdd, samplingRatio=1.0))

# Exibe o DataFrame na tela
df.foreach(lambda x: print(x.show()))

# Inicia o processo do Spark Streaming
ssc.start()
ssc.awaitTermination()
