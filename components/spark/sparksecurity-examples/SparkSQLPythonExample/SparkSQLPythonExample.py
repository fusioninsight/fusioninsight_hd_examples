# -*- coding:utf-8 -*-

import sys
from pyspark.sql import SQLContext
from pyspark import SparkConf,SparkContext


def contains(str1, substr1):
    if substr1 in str1:
        return True
    return False


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print "Usage: SparkSQLPythonExample.py <file>"
        exit(-1)

    # 初始化SQLContext
    conf = SparkConf().setAppName("CollectFemaleInfo")
    sc = SparkContext(conf=conf)
    sqlCtx = SQLContext(sc)

    # RDD转换为DataFrame
    inputPath = sys.argv[1]
    inputRDD = sc.textFile(inputPath)\
        .map(lambda line: line.split(","))\
        .map(lambda dataArr: (dataArr[0], dataArr[1], int(dataArr[2])))\
        .collect()
    df = sqlCtx.createDataFrame(inputRDD)

    # 注册表
    df.registerTempTable("FemaleInfoTable")

    # 执行SQL查询并显示结果
    FemaleTimeInfo = sqlCtx.sql("select _1 as name, sum(_3) as time from FemaleInfoTable where _2 = 'female' group by _1 having sum(_3)>120").show()

    sc.stop()
