package com.huawei.bigdata.spark.examples

import java.util.Properties

import com.huawei.spark.streaming.kafka.KafkaWriter._
import kafka.producer.KeyedMessage
import kafka.serializer.StringDecoder
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

import com.huawei.hadoop.security.LoginUtil

/**
  * Consumes messages from one or more topics in Kafka.
  * <batchTime> is the Spark Streaming batch duration in seconds.
  * <windowTime> is the width of the window
  * <topics> is a list of one or more kafka topics to consume from
  * <brokers> is for bootstrapping and the producer will only use it for getting metadata
  */

object FemaleInfoCollectionKafka {
  def main(args: Array[String]) {
    val userPrincipal = "sparkuser"
    val userKeytabPath = "/opt/FIclient/user.keytab"
    val krb5ConfPath = "/opt/FIclient/KrbClient/kerberos/var/krb5kdc/krb5.conf"

    val hadoopConf: Configuration = new Configuration();
    LoginUtil.login(userPrincipal, userKeytabPath, krb5ConfPath, hadoopConf)

    val Array(checkPointDir, batchTime, windowTime, topics, brokers) = args
    val batchDuration = Seconds(batchTime.toInt)
    val windowDuration = Seconds(windowTime.toInt)

    // Create a Streaming startup environment.
    val sparkConf = new SparkConf().setAppName("DataSightStreamingExample")
    val ssc = new StreamingContext(sparkConf, batchDuration)

    // Configure the CheckPoint directory for the Streaming. This parameter is mandatory because of existence of the window concept.
    ssc.checkpoint(checkPointDir)

    // Get the list of topic used by kafka
    val topicsSet = topics.split(",").toSet

    // Create direct kafka stream with brokers and topics
    // Receive data from the Kafka and generate the corresponding DStream
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet).map(_._2)

    // Obtain field properties in each row.
    val records = lines.map(getRecord)

    // Filter data about the time that female netizens spend online.
    val femaleRecords = records.filter(_._2 == "female")
      .map(x => (x._1, x._3))

    // Aggregate the total time that each female netizen spends online within a time frame.
    val aggregateRecords = femaleRecords
      .reduceByKeyAndWindow(_ + _, _ - _, windowDuration)

    // Filter data about users whose consecutive online duration exceeds the threshold
    val upTimeUser = aggregateRecords
      .filter(_._2 > 0.9 * windowTime.toInt)

    // Configure the properties of kafka
    val producerConf = new Properties()
    producerConf.put("serializer.class", "kafka.serializer.DefaultEncoder")
    producerConf.put("key.serializer.class", "kafka.serializer.StringEncoder")
    producerConf.put("metadata.broker.list", brokers)
    producerConf.put("request.required.acks", "1")

    // Send the result to the Kafka as a message, which is named as default and is randomly sent to a partition.
    upTimeUser.writeToKafka(producerConf, (x: (String, Int)) => {
      val t = x._1 + "," + x._2.toString
      new KeyedMessage[String, Array[Byte]]("default", null, t.getBytes())
    })

    // start Streaming
    ssc.start()
    ssc.awaitTermination()
  }

  // get enums of record
  def getRecord(line: String): (String, String, Int) = {
    val elems = line.split(",")
    val name = elems(0)
    val sexy = elems(1)
    val time = elems(2).toInt
    (name, sexy, time)
  }
}
