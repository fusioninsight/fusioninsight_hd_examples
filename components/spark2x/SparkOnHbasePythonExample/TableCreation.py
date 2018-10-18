from py4j.java_gateway import java_import
from pyspark.sql import SparkSession

spark = SparkSession\
        .builder\
        .appName("py4jTesting")\
        .getOrCreate()

java_import(spark._jvm, 'com.huawei.bigdata.spark.examples.TableCreation')


spark._jvm.TableCreation().createtable()

spark.stop()
