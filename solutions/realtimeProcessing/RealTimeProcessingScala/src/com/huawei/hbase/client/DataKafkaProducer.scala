package com.huawei.hbase.client

import java.util.Properties
import java.util.logging.Logger

import com.huawei.hadoop.security.LoginUtil
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.{SparkConf, SparkContext}

object DataKafkaProducer {

  def main(args: Array[String]): Unit = {

    val log = Logger.getLogger("test")

    val propertie = new Properties()
    val in = this.getClass.getClassLoader().getResourceAsStream("ParamsConf.properties")
    propertie.load(in)
    //安全认证
    val userPrincipal = propertie.getProperty("userPrincipal")
    val userKeytabPath = propertie.getProperty("userKeytabPath")
    val krb5ConfPath = propertie.getProperty("krb5ConfPath")
    val hadoopConf: Configuration = new Configuration()
    LoginUtil.login(userPrincipal, userKeytabPath, krb5ConfPath, hadoopConf)
    log.info("slw---the kerbros is successful")

    val conf = new SparkConf().setAppName("kafka-producer")
    val sc = new SparkContext(conf)



    //defined topic
    log.info("slw---begin to load topic")
    val topics = propertie.getProperty("topics")

    val hotelTopic = (topics.split(","))(0)
    val netTopic = (topics.split(","))(1)
    val gateTopic = (topics.split(","))(2)


    log.info("slw---begin to load brokers")
    val brokers = propertie.getProperty("brokers")
    //println(brokers.toString)

    //配置kafka的producer的属性
    //property.put("acks","0")
    propertie.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,brokers)

    propertie.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")

    propertie.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")


    //加载文件路径
    log.info("slw---begin to load file path")
    val hotelInputPath = propertie.getProperty("hotelDataPath")
    val netInputPath = propertie.getProperty("netDataPath")
    val checkInputPath = propertie.getProperty("checkDataPath")


      //spark数据入口，返回RDD
      val hotelFile = sc.textFile(hotelInputPath)
      val netFile = sc.textFile(netInputPath)
      val checkFile = sc.textFile(checkInputPath)

      //println(hotelFile.foreach( line => )
      //    print(hotelFile.collect())
      //定义key为对应的信息来源，以便于从对应的topic中拉取数据

      hotelFile.foreachPartition(lines => {
        val producer = new KafkaProducer[String,String](propertie)
        while (lines.hasNext){
          var  line = lines.next()
          println(line)
          println("开始发送--hotel--数据")
          val message = new ProducerRecord[String,String](hotelTopic,"hotel",line)
          producer.send(message)
          println("发送成功")
        }
        producer.close()
      })

      netFile.foreachPartition(lines => {
        val producer = new KafkaProducer[String,String](propertie)
        while (lines.hasNext){
          var  line = lines.next()
          println(line)
          println("开始发送--netBar--数据")
          val message = new ProducerRecord[String,String](netTopic,"netBar",line)
          producer.send(message)
          println("发送成功")
        }
        producer.close()
      })

      checkFile.foreachPartition(lines => {
        val producer = new KafkaProducer[String,String](propertie)
        while (lines.hasNext){
          var  line = lines.next()
          println(line)
          println("开始发送--check--数据")
          val message = new ProducerRecord[String,String](gateTopic,"check",line)
          producer.send(message)
          println("发送成功")
        }
        producer.close()
      })

        sc.stop()
      }
}
