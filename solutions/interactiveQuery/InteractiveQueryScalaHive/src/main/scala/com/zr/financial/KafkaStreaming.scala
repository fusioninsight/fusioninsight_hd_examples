package com.zr.financial

import java.util.Properties

import main.scala.com.zr.utils.{HdfsOperation, LoginUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object KafkaStreaming {
  def main(args: Array[String]): Unit = {
    val propertie = new Properties()
    val in = this.getClass.getClassLoader().getResourceAsStream("Fproducer.properties")
    propertie.load(in)
    //安全认证
    val userPrincipal = propertie.getProperty("userPrincipal")
    val userKeytabPath = propertie.getProperty("userKeytabPath")
    val krb5ConfPath = propertie.getProperty("krb5ConfPath")
    val hadoopConf: Configuration = new Configuration()
    LoginUtil.login(userPrincipal, userKeytabPath, krb5ConfPath, hadoopConf)

    val conf = new SparkConf().setMaster("local[2]").setAppName("kafka")
    val ssc = new StreamingContext(conf, Seconds(2))
    Class.forName("org.apache.hadoop.hive.conf.HiveConf")
    val sparkHive = SparkSession.builder().appName("gg").master("local").enableHiveSupport().getOrCreate()
    ssc.checkpoint(propertie.getProperty("chekPath"))
    //配置kafka信息
    val topics = propertie.getProperty("topic").split("\\|").toSet //topic
    println(propertie.getProperty("topic"))
    val groupId = propertie.getProperty("groupId")
    val brokers = propertie.getProperty("brokers")
    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> brokers,
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> groupId
    )

    /**
      * 银行帐号bankCount、帐号姓名name、身份证IDCard、交易日期tradeDate、交易金额tradeAmount、
      * 交易类型tradeType（柜台转账/网银转账）、
      * 目标帐号desBankCount、目标帐号姓名desName、目标帐号身份desIDCard、备注remark。
      */
    import sparkHive.sql

    sql("create database if not exists qlz")
    sql("use qlz")
    //    创建表存储交易信息tradeInfo
    sql("create table if not exists tradeInfo(bankCount STRING,name STRING,IDCard STRING," +
      "tradeDate STRING,tradeAmount INT,tradeType STRING,desBankCount STRING,desName STRING ,desIDCard STRING,remark STRING) row format delimited fields terminated by '|'")
    //创建账户信息表
    sql("create table if not exists accountInfo(bankCount STRING,name STRING,IDCard STRING) row format delimited fields terminated by '|'")
    val locationStrategy = LocationStrategies.PreferConsistent
    val consumerStrategies = ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    //拉取kafka的数据
    val kafkaStream = KafkaUtils.createDirectStream[String, String](ssc, locationStrategy, consumerStrategies)
    //交易记录的存储路径
    val tradeInfoPath = propertie.getProperty("tradeInfoPath")
    println(tradeInfoPath)
    //账户信息的存储
    val accountInfoPath = propertie.getProperty("accountInfoPath")
    println(accountInfoPath)
    if (!HdfsOperation.queryFile(tradeInfoPath)) {
      HdfsOperation.createFile(tradeInfoPath)
    }
    if (!HdfsOperation.queryFile(accountInfoPath)) {
      HdfsOperation.createFile(accountInfoPath)
    }
    kafkaStream.foreachRDD(rdd => {
      rdd.foreach(x => {
        //交易记录
        HdfsOperation.appendContext(tradeInfoPath, x.value() + "\r\n")
        val s = x.value().split("|")
        //账户信息
        HdfsOperation.appendContext(accountInfoPath, s(0) + "|" + s(1) + "|" + s(2) + "\r\n")
        HdfsOperation.appendContext(accountInfoPath, s(6) + "|" + s(7) + "|" + s(8) + "\r\n")
      })
    })

    //加载交易数据到hive
    //    sql(s"load data inpath '$tradeInfoPath' into table tradeInfo")
    //加载账户数据到hive
    //    sql(s"load data inpath '$tradeInfoPath' into table accountInfo")
    ssc.start()
    ssc.awaitTermination()
  }

}
