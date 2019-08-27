package com.huawei.bigdata.spark.examples

import com.huawei.hadoop.security.LoginUtil
import org.apache.carbondata.streaming.parser.CarbonStreamParser
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{ProcessingTime, StreamingQuery}

/**
  * Consumes messages from one or more topics in Kafka.
  * <checkPointDir> is the Spark Streaming checkpoint directory.
  * <brokers> is for bootstrapping and the producer will only use it for getting metadata
  * <topics> is a list of one or more kafka topics to consume from
  * <batchTime> is the Spark Streaming batch duration in seconds.
  */
object KafkaToCarbonStream {

  def main(args: Array[String]) {
    createContext(args)

    //The Streaming system starts.
  }

  def createContext(args : Array[String]) = {
    //获取用户keytab 信息，注意路径
    val userPrincipal = "sparkuser"
    val userKeytabPath = "/opt/FIclient/user.keytab"
    val krb5ConfPath = "/opt/FIclient/KrbClient/kerberos/var/krb5kdc/krb5.conf"

    //登陆认证
    val hadoopConf: Configuration = new Configuration()
    LoginUtil.login(userPrincipal, userKeytabPath, krb5ConfPath, hadoopConf)

    //streamTableName  CarbonData 流式表名
    //brokers  kafka brokers 例如：10.90.46.60:21005,10.90.46.61:21005,10.90.46.62:21005
    //topics  kafka topics 名称， 例如：topic02
    //batchTime  流式处理的批次时间，单位秒，例如10
    //checkpoint 流处理checkPoint 地址，hdfs 路径，例如：/tmp/test01
    val Array(streamTableName, brokers, topics, batchTime, checkpoint) = args

    val spark = SparkSession
      .builder()
      .appName("kafkaToCarbonDataStream").config(new SparkConf()).getOrCreate()

    var qry: StreamingQuery = null
    try {
      //从kafka获取数据
      val source = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", brokers)
        .option("subscribe", topics)
        .load()

      //数据写入到CarbonData 流式表
      import spark.implicits._
      qry = source
        .selectExpr("CAST(value AS STRING)")
        .as[String]
        .writeStream
        .format("carbondata")
        .trigger(ProcessingTime(batchTime.toLong * 1000))
        .option("checkpointLocation", checkpoint)
        .option("bad_records_logger_enable", false)
        .option("dbName", "default")
        .option("tableName", streamTableName)
        .option(CarbonStreamParser.CARBON_STREAM_PARSER, CarbonStreamParser.CARBON_STREAM_PARSER_CSV)
        .start()

      qry.awaitTermination()
    } catch {
      case ex =>
        ex.printStackTrace()
        println("Wrong reading and writing streaming data")
    } finally {
      qry.stop()
    }
  }
}
