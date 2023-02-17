
# criando um data frame

from pyspark.sql import SparkSession

# Cria um DataFrame de exemplo
spark = SparkSession.builder.appName("rename_column").getOrCreate()
data = [("João", 25), ("Maria", 30), ("José", 40)]
df = spark.createDataFrame(data, ["nome", "idade"])

df.show()


spark.stop()