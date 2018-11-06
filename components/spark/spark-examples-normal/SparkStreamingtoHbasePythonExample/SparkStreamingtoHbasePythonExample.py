# -*- coding:utf-8 -*-
"""
【说明】【本样例还有问题】
(1)由于pyspark不提供Hbase相关api,本样例使用Python调用Java的方式实现,将Java代码打包为jar之后添加到classpath中
(2)如果使用yarn-client模式运行,请确认Spark客户端Spark/spark/conf/spark-defaults.conf中
   spark.hbase.obtainToken.enabled参数配置为true
(3)使用yarn-client模式运行时，由于Pyspark1.5源码中的bug(https://issues.apache.org/jira/browse/SPARK-5185)
   最好同时使用--jars和--driver-class-path参数加载jar包
"""

import sys
from py4j.java_gateway import java_import
from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print "Usage: SparkOnStreamingToHbasePythonExample.py <checkPointDir> <topics> <brokers>"
        exit(-1)

    # 初始化SparkContext
    conf = SparkConf().setAppName("SparkOnStreamingToHbase")
    sc = SparkContext(conf = conf)
	
	# 向sc._jvm中导入要运行的类
    java_import(sc._jvm, 'com.huawei.bigdata.spark.examples.streaming.SparkOnStreamingToHbase')

	# 创建类实例并调用方法
    sc._jvm.SparkOnStreamingToHbase().streamingtohbase(sc._jsc, sys.argv[1], sys.argv[2], sys.argv[3])
	