package com.huawei.hbase.client


import java.util.Properties
import java.util.logging.Logger

import com.huawei.hadoop.security.LoginUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object SparkStreamingToHbase {


  def main(args: Array[String]): Unit = {

    //加载配置文件
    val property = new Properties()
    val in = this.getClass.getClassLoader().getResourceAsStream("ParamsConf.properties")
    property.load(in)


    //进行安全认证

    val userPrincipal = property.getProperty("userPrincipal")
    val userKeytabPath = property.getProperty("userKeytabPath")
    val krb5ConfPath = property.getProperty("krb5ConfPath")

    val hadoopConf: Configuration = new Configuration();
    LoginUtil.login(userPrincipal, userKeytabPath, krb5ConfPath, hadoopConf);


    //建立spark streaming启动环境


    val checkPoint = property.getProperty("checkPoint")
    val groupId = property.getProperty("groupId")
    val sparkConf = new SparkConf().setAppName("spark to hbase")
    //sparkConf.set("spark.driver.allowMultipleContexts","true")

    val ssc = new StreamingContext(sparkConf,Seconds(2))

    //加载chenkpoint目录
    ssc.checkpoint(checkPoint)
    //println(checkPoint.toString)
    val brokers = property.getProperty("brokers")
    val kafkaParams = Map[String,String]("bootstrap.servers" -> brokers
      ,"value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> groupId)

    val topic = property.getProperty("topics").split(",").toSet
    val locationStrategy = LocationStrategies.PreferConsistent
    val consumerStrategies = ConsumerStrategies.Subscribe[String, String](topic, kafkaParams)
    val lines = KafkaUtils.createDirectStream[String, String](ssc, locationStrategy, consumerStrategies)

    //取出嫌疑人员得信息
    val hiveContext = SparkSession.builder().appName("gg").enableHiveSupport().getOrCreate()
    import hiveContext.implicits._
    hiveContext.sql("use test001")
    val controlDataSet = hiveContext.sql("SELECT * FROM controldata")
    val pp = controlDataSet.map(x => (x(5).toString,(x(0).toString,x(1).toString,x(2).toString,x(3).toString,x(4).toString,x(6).toString))).rdd

    //val pp = controlDataSet.toString().split(",")

    //对两种数据循环判断，如果符合条件，则将布控信息存入hbase
    lines.foreachRDD(rdd => {
      val key = rdd.map( x => x.key())
      //取出发送数据的每个RDD的key，判断数据类型
      if (key.equals("hotel")){
        //对数据做（K,V）形式的map
        val value = rdd.map(v => v.value().split(","))
        val hrdd = value.map(h => (h(1),(h(0),h(2),h(3),h(4),h(5),h(6),h(7))))

        //对布控数据和布控人员做join，匹配不到的会直接过滤掉
        val joinData = (hrdd.join(pp)).map(x =>
          (x._2._1._1,x._1,x._2._1._2,x._2._1._3,
          x._2._1._4,x._2._1._5,x._2._1._6,x._2._1._7))

        //遍历join后的数据并写入hbase
        joinData.foreachPartition( j=> {
          val logger = Logger.getLogger("event-alarm")
          val myConf = HBaseConfiguration.create()
          val tableName = "ControlInfo"
          val zkQuorun = property.getProperty("zk.quorum")
          val zkPort = property.getProperty("zk.port")
          myConf.set("hbase.zookeeper.quorum",zkQuorun)
          myConf.set("hbase.zookeeper.property.clientPort",zkPort)
          myConf.set("hbase.defaults.for.version.skip","true")
          val myTable = new HTable(myConf,TableName.valueOf(tableName))
          myTable.setAutoFlush(false,false)
          myTable.setWriteBufferSize(3*1024*1024)
          while (j.hasNext){
            var s = j.next()
            val p = new Put(Bytes.toBytes(s._2))
            p.add("hotel".getBytes, "name".getBytes, Bytes.toBytes(s._1))
            p.add("hotel".getBytes, "identId".getBytes, Bytes.toBytes(s._2))
            p.add("hotel".getBytes, "age".getBytes, Bytes.toBytes(s._3))
            p.add("hotel".getBytes, "sex".getBytes, Bytes.toBytes(s._4))
            p.add("hotel".getBytes, "hotelAddr".getBytes, Bytes.toBytes(s._5))
            p.add("hotel".getBytes, "startTime".getBytes, Bytes.toBytes(s._6))
            p.add("hotel".getBytes, "leaveTime".getBytes, Bytes.toBytes(s._7))
            p.add("hotel".getBytes, "togetherPer".getBytes, Bytes.toBytes(s._8))
            myTable.put(p)
          }

          myTable.flushCommits()
          myTable.close()
        })
      }

      if (key.equals("netBar")){
        val value = rdd.map(v => v.value().split(","))
        val hrdd = value.map(h => (h(1),(h(0),h(2),h(3),h(4),h(5),h(6))))
        val joinData = (hrdd.join(pp)).map(x =>
          (x._2._1._1,x._1,x._2._1._2,x._2._1._3,
            x._2._1._4,x._2._1._5,x._2._1._6))
        joinData.foreachPartition( j=> {
          val logger = Logger.getLogger("event-alarm")
          val myConf = HBaseConfiguration.create()
          val tableName = "ControlInfo"
          val zkQuorun = property.getProperty("zk.quorum")
          val zkPort = property.getProperty("zk.port")
          myConf.set("hbase.zookeeper.quorum",zkQuorun)
          myConf.set("hbase.zookeeper.property.clientPort",zkPort)
          myConf.set("hbase.defaults.for.version.skip","true")
          val myTable = new HTable(myConf,TableName.valueOf(tableName))
          myTable.setAutoFlush(false,false)
          myTable.setWriteBufferSize(3*1024*1024)
          while (j.hasNext){
            var s = j.next()
            val p = new Put(Bytes.toBytes(s._2))
            p.add("netBar".getBytes, "name".getBytes, Bytes.toBytes(s._1))
            p.add("netBar".getBytes, "identId".getBytes, Bytes.toBytes(s._2))
            p.add("netBar".getBytes, "age".getBytes, Bytes.toBytes(s._3))
            p.add("netBar".getBytes, "sex".getBytes, Bytes.toBytes(s._4))
            p.add("netBar".getBytes, "netBarAddr".getBytes, Bytes.toBytes(s._5))
            p.add("netBar".getBytes, "netDate".getBytes, Bytes.toBytes(s._6))
            p.add("netBar".getBytes, "netTime".getBytes, Bytes.toBytes(s._7))
            myTable.put(p)
          }

          myTable.flushCommits()
          myTable.close()
        })
      }

      if (key.equals("check")){
        val value = rdd.map(v => v.value().split(","))
        val hrdd = value.map(h => (h(1),(h(0),h(2),h(3),h(4),h(5),h(6))))
        val joinData = (hrdd.join(pp)).map(x =>
          (x._2._1._1,x._1,x._2._1._2,x._2._1._3,
            x._2._1._4,x._2._1._5,x._2._1._6))
        joinData.foreachPartition( j=> {
          val logger = Logger.getLogger("event-alarm")
          val myConf = HBaseConfiguration.create()
          val tableName = "ControlInfo"
          val zkQuorun = property.getProperty("zk.quorum")
          val zkPort = property.getProperty("zk.port")
          myConf.set("hbase.zookeeper.quorum",zkQuorun)
          myConf.set("hbase.zookeeper.property.clientPort",zkPort)
          myConf.set("hbase.defaults.for.version.skip","true")
          val myTable = new HTable(myConf,TableName.valueOf(tableName))
          myTable.setAutoFlush(false,false)
          myTable.setWriteBufferSize(3*1024*1024)
          while (j.hasNext){
            var s = j.next()
            val p = new Put(Bytes.toBytes(s._2))
            p.add("gateCheck".getBytes, "name".getBytes, Bytes.toBytes(s._1))
            p.add("gateCheck".getBytes, "identId".getBytes, Bytes.toBytes(s._2))
            p.add("gateCheck".getBytes, "age".getBytes, Bytes.toBytes(s._3))
            p.add("gateCheck".getBytes, "sex".getBytes, Bytes.toBytes(s._4))
            p.add("gateCheck".getBytes, "checkLocation".getBytes, Bytes.toBytes(s._5))
            p.add("gateCheck".getBytes, "checkTime".getBytes, Bytes.toBytes(s._6))
            p.add("gateCheck".getBytes, "outType".getBytes, Bytes.toBytes(s._7))
            myTable.put(p)
          }

          myTable.flushCommits()
          myTable.close()
        })
      }
    })
    ssc.start()
    ssc.awaitTermination()

  }
}
