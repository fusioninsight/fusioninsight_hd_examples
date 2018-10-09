package com.zr.financialElk

import java.io.{File, FileInputStream}
import java.sql.{PreparedStatement, SQLException}
import java.util.Properties

import main.scala.com.zr.utils.{ElkConnection, LoginUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}


object KafkaStreaming
{
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.zookeeper").setLevel(Level.WARN)

    def main(args: Array[String]): Unit =
    {
        //加载配置
        val propertie = new Properties()
        val in = this.getClass.getClassLoader().getResourceAsStream("Fproducer.properties")
        propertie.load(in)
        //进行安全认证
        val userPrincipal = propertie.getProperty("userPrincipal")
        val userKeytabPath = propertie.getProperty("userKeytabPath")
        val krb5ConfPath = propertie.getProperty("krb5ConfPath")
        val hadoopConf: Configuration = new Configuration()
        LoginUtil.setJaasFile(userPrincipal, userKeytabPath)
        LoginUtil.login(userPrincipal, userKeytabPath, krb5ConfPath, hadoopConf)
        //EL连接用户名和密码
        val userName = propertie.getProperty("userName")
        val pw = propertie.getProperty("password")
        //创建elk连接
        val conn = ElkConnection.getConnection(userName, pw)
        //spark配置加载
        val conf = new SparkConf().setMaster("local[3]").setAppName("kafka")
        val ssc = new StreamingContext(conf, Seconds(2))
        ssc.checkpoint(propertie.getProperty("chekPath"))
        //kafka的topic
        val topics = propertie.getProperty("topic").split("\\|").toSet
        //kafka集群配置
        val groupId = propertie.getProperty("groupId")
        val brokers = propertie.getProperty("brokers")
        val kafkaParams = Map[String, String](
            "bootstrap.servers" -> brokers,
            "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
            "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
            "group.id" -> groupId,
            "kerberos.domain.name" -> "hadoop.hadoop.com"
            //            "security.protocol"->"SASL_PLAINTEXT",
            //            "sasl.kerberos.service.name" -> "kafka"
        )
        /**
          * 银行帐号bankCount、帐号姓名name、身份证IDCard、交易日期tradeDate、交易金额tradeAmount、
          * 交易类型tradeType（柜台转账/网银转账）、
          * 目标帐号desBankCount、目标帐号姓名desName、目标帐号身份desIDCard、备注remark。
          */
        //    创建表存储交易信息tradeInfo
        val sqlText1 = "CREATE TABLE IF NOT EXISTS tradeInfo(bankCount VARCHAR(128),name VARCHAR(128)," +
            "IDCard VARCHAR(128),tradeDate VARCHAR(128),tradeAmount INTEGER,tradeType VARCHAR(32)," +
            "desBankCount VARCHAR(128),desName VARCHAR(128),desIDCard VARCHAR(128),remark VARCHAR(128)) tablespace hdfs;"
        ElkConnection.createTable(conn, sqlText1)
        //创建账户信息表
        val sqlText2 = "CREATE TABLE IF NOT EXISTS accountInfo(bankCount VARCHAR(128),name VARCHAR(128),IDCard VARCHAR(128)) tablespace hdfs;"
        ElkConnection.createTable(conn, sqlText2)
        val locationStrategy = LocationStrategies.PreferConsistent
        val consumerStrategies = ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
        //拉取kafka数据
        val kafkaStream = KafkaUtils.createDirectStream[String, String](ssc, locationStrategy, consumerStrategies).map(_.value())


        kafkaStream.foreachRDD(rdd => {
            if (!rdd.isEmpty()) {
                println("--------------")
                rdd.collect().foreach(println)
                val tradeInfo = rdd.map(_.toString().split("\\|")).filter(x => x.length == 10).map(x => (x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9)))
                val accountInfo1 = rdd.map(_.toString().split("\\|")).filter(x => x.length == 10).map(x => (x(0), x(1), x(2)))
                val accountInfo2 = rdd.map(_.toString().split("\\|")).filter(x => x.length == 10).map(x => (x(6), x(7), x(8)))
                val accountInfo = accountInfo1.union(accountInfo1)

                accountInfo1.collect().foreach(println)
                accountInfo2.collect().foreach(println)

                tradeInfo.foreach(x => {
                    var pst: PreparedStatement = null
                    try { //添加数据。
                        var connElk = ElkConnection.getConnection(userName, pw)
                        pst = connElk.prepareStatement("INSERT INTO tradeInfo(bankCount,name,IDCard,tradeDate,tradeAmount" +
                            ",tradeType,desBankCount,desName,desIDCard,remark) VALUES (?,?,?,?,?,?,?,?,?,?)")
                        pst.setString(1, x._1.toString)
                        pst.setString(2, x._2.toString)
                        pst.setString(3, x._3.toString)
                        pst.setString(4, x._4.toString)
                        pst.setInt(5, x._5.toInt)
                        pst.setString(6, x._6.toString)
                        pst.setString(7, x._7.toString)
                        pst.setString(8, x._8.toString)
                        pst.setString(9, x._9.toString)
                        pst.setString(10, x._10.toString)
                        pst.addBatch()
                        //执行批量插入操作。
                        pst.executeBatch()
                        pst.close
                    } catch {
                        case e: SQLException =>
                            if (pst != null) try
                                pst.close
                            catch {
                                case e1: SQLException =>
                                    e1.printStackTrace()
                            }
                            e.printStackTrace()
                    }
                })
                accountInfo.foreach(x => {
                    var pst: PreparedStatement = null
                    try { //添加数据
                        var connElk = ElkConnection.getConnection(userName, pw)
                        pst = connElk.prepareStatement("INSERT INTO accountinfo(bankCount,name,IDCard) VALUES (?,?,?)")
                        pst.setString(1, x._1.toString)
                        pst.setString(2, x._2.toString)
                        pst.setString(3, x._3.toString)
                        pst.addBatch()
                        //执行批量插入操作。
                        pst.executeBatch()
                        pst.close
                    } catch {
                        case e: SQLException =>
                            if (pst != null) try
                                pst.close
                            catch {
                                case e1: SQLException =>
                                    e1.printStackTrace()
                            }
                            e.printStackTrace()
                    }
                })
            }
        })
        ssc.start()
        ssc.awaitTermination()
    }
}
