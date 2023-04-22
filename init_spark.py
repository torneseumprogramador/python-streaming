# python -m pip install findspark
# python -m pip install pyspark

import os

os.environ["JAVA_HOME"] = "/Library/Java/JavaVirtualMachines/jdk-18.jdk/Contents/Home/"
os.environ["SPARK_HOME"] = "/Users/danilo/Downloads/spark-3.3.2-bin-hadoop3"
import findspark

findspark.init()

