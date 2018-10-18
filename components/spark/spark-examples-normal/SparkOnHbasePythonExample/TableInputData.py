from py4j.java_gateway import java_import
from pyspark.sql import SparkSession


spark = SparkSession\
        .builder\
        .appName("CollectInfo")\
        .getOrCreate()

java_import(spark._jvm, 'com.huawei.bigdata.spark.examples.TableInputData')

javaRdd = spark.sparkContext.textFile("/tmp/input").map(lambda s: s.split(","))._to_java_object_rdd()

spark._jvm.TableInputData().writetable(javaRdd)

spark.stop()
