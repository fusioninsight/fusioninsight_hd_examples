package main.scala.com.zr.police

import java.io.File
import java.util.Properties

import main.scala.com.zr.utils.LoginUtil
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.{SparkConf, SparkContext}


object DataProducer {

  def main(args: Array[String]): Unit = {
    //加载配置文件数据
    val propertie = new Properties()
    val in = this.getClass.getClassLoader().getResourceAsStream("Fproducer.properties")
    propertie.load(in)
    //安全认证
    val userPrincipal = propertie.getProperty("userPrincipal")
    val userKeytabPath = propertie.getProperty("userKeytabPath")
    val krb5ConfPath = propertie.getProperty("krb5ConfPath")
    val hadoopConf: Configuration = new Configuration()
    LoginUtil.login(userPrincipal, userKeytabPath, krb5ConfPath, hadoopConf)
    //加载spark配置
    System.setProperty("hadoop.home.dir", "D:\\DevelopEnvirement\\hadoop")
    val conf = new SparkConf().setAppName("write to kafka").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val chekPath = propertie.getProperty("chekPath")
    println(chekPath)
    //创建topic
    val topic = propertie.getProperty("topic")
    //配置kafka集群所有节点
    val brokers = propertie.getProperty("brokers")
    val property = new Properties()
    property.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    property.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    property.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    //原始数据文件路径
    val inputPath = propertie.getProperty("inputPath")
      if (inputPath.contains("hotel")) {
        val file = sc.textFile(inputPath)
        file.foreach(x => {
          println("文件内容: " + x)
        })
        file.foreachPartition((partisions: Iterator[String]) => {
          //创建生产者
          val producer = new KafkaProducer[String, String](property)
          partisions.foreach(x => {
            val message = new ProducerRecord[String, String](topic.toString, "hotel", x.toString)
            //生产者将消息写入kafka
            try {
              producer.send(message)
              print("producer 工作完毕")
            }
            catch {
              case e: Exception => {
                print("Task not serialized")
              }
            }
          })
          producer.close()
        })
      } else if (inputPath.contains("internet")) {
        val file = sc.textFile(inputPath)
        file.foreach(x => {
          println("文件内容: " + x)
        })
        file.foreachPartition((partisions: Iterator[String]) => {
          //创建生产者
          val producer = new KafkaProducer[String, String](property)
          partisions.foreach(x => {
            val message = new ProducerRecord[String, String](topic.toString, "internet", x.toString)
            //生产者将消息写入kafka
            try {
              producer.send(message)
              print("producer 工作完毕")
            }
            catch {
              case e: Exception => {
                print("Task not serialized")
              }
            }
          })
          producer.close()
        })
      } else {
        val file = sc.textFile(inputPath)
        file.foreach(x => {
          println("文件内容: " + x)
        })
        file.foreachPartition((partisions: Iterator[String]) => {
          //创建生产者
          val producer = new KafkaProducer[String, String](property)
          partisions.foreach(x => {
            val message = new ProducerRecord[String, String](topic.toString, "bayonet", x.toString)
            //生产者将消息写入kafka
            try {
              producer.send(message)
              print("producer 工作完毕")
            }
            catch {
              case e: Exception => {
                print("Task not serialized")
              }
            }
          })
          producer.close()
        })
      }
  }
}
