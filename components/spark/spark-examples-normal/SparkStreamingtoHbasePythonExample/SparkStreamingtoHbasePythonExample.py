# -*- coding:utf-8 -*-
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

    java_import(sc._jvm, 'com.huawei.bigdata.spark.examples.streaming.SparkOnStreamingToHbase')

    sc._jvm.SparkOnStreamingToHbase().streamingtohbase(sc._jsc, sys.argv[1], sys.argv[2], sys.argv[3])
