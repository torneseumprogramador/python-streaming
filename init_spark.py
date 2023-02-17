# python -m pip install findspark
# python -m pip install pyspark

import os
os.environ["JAVA_HOME"] = "/Library/Java/JavaVirtualMachines/jdk-18.jdk/Contents/Home/"
os.environ["SPARK_HOME"] = "/Users/Danilo/Desktop/python_bi/streaming-python/spark-3.3.1-bin-hadoop3"
import findspark
findspark.init()

