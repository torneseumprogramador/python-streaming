from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("desafio_data_science").getOrCreate()

df = spark.read.format("csv").option("header", True).option("delimiter", ",").load("nomes.csv") # Especifica o caminho para o arquivo
df.show()
