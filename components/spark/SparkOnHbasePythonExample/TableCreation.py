# -*- coding:utf-8 -*-
"""
【说明】
(1)由于pyspark不提供Hbase相关api,本样例使用Python调用Java的方式实现,将Java代码打包为jar之后添加到classpath中
(2)如果使用yarn-client模式运行,请确认Spark客户端Spark/spark/conf/spark-defaults.conf中
   spark.hbase.obtainToken.enabled参数配置为true
(3)使用yarn-client模式运行时，由于Pyspark1.5源码中的bug(https://issues.apache.org/jira/browse/SPARK-5185)
   最好同时使用--jars和--driver-class-path参数加载jar包
"""

from py4j.java_gateway import java_import
from pyspark.sql import SparkSession

# 创建SparkContext
conf = SparkConf().setAppName("TableCreation")
spark = SparkContext(conf=conf)

# 向sc._jvm中导入要运行的类
java_import(spark._jvm, 'com.huawei.bigdata.spark.examples.TableCreation')

# 创建类实例并调用方法
spark._jvm.TableCreation().createtable()

# 停止SparkContext
spark.stop()
