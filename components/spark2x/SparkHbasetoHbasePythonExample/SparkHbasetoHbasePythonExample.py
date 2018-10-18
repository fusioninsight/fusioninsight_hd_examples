from py4j.java_gateway import java_import
# from pyspark.sql import SparkSession
from pyspark import SparkConf,SparkContext

# spark = SparkSession\
#         .builder\
#         .appName("SparkHbasetoHbase") \
#         .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
#         .config("spark.kryo.registrator", "com.huawei.bigdata.spark.examples.MyRegistrator") \
#         .getOrCreate()

conf = SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
        .set("spark.kryo.registrator","com.huawei.bigdata.spark.examples.MyRegistrator")
spark = SparkContext(conf=conf)

java_import(spark._jvm, 'com.huawei.bigdata.spark.examples.SparkHbasetoHbase')

spark._jvm.SparkHbasetoHbase().hbasetohbase(spark._jsc)

