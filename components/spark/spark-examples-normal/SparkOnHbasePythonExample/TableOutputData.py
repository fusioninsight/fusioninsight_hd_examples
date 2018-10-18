from py4j.java_gateway import java_import
from pyspark.sql import SparkSession

spark = SparkSession\
        .builder\
        .appName("CollectInfo")\
        .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")\
        .config("spark.kryo.registrator", "com.huawei.bigdata.spark.examples.MyRegistrator")\
        .getOrCreate()

java_import(spark._jvm, 'com.huawei.bigdata.spark.examples.TableOutputData')

spark._jvm.TableOutputData().readtable(spark._jsc)
