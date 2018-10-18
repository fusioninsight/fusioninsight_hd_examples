from py4j.java_gateway import java_import
from pyspark.sql import SparkSession

spark = SparkSession\
        .builder\
        .appName("SparkHivetoHbase") \
        .getOrCreate()

java_import(spark._jvm, 'com.huawei.bigdata.spark.examples.SparkHivetoHbase')

spark._jvm.SparkHbasetoHbase().hbasetohbase(spark._jsc)
