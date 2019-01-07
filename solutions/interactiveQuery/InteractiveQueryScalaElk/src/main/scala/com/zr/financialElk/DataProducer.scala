package com.zr.financialElk

import java.util.Properties

import main.scala.com.zr.utils.LoginUtil
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object DataProducer
{

    def main(args: Array[String]): Unit =
    {
        //读取配置文件
        val a = "dasdasd";
        val propertie = new Properties()
        val in = this.getClass.getClassLoader().getResourceAsStream("Fproducer.properties")
        propertie.load(in)
        //安全认证
        val userPrincipal = propertie.getProperty("userPrincipal")
        val userKeytabPath = propertie.getProperty("userKeytabPath")
        val krb5ConfPath = propertie.getProperty("krb5ConfPath")
        val hadoopConf: Configuration = new Configuration()
        LoginUtil.setJaasFile(userPrincipal, userKeytabPath)
        LoginUtil.login(userPrincipal, userKeytabPath, krb5ConfPath, hadoopConf)

        //加载spark配置
        val conf = new SparkConf().setAppName("write to kafka").setMaster("local[1]")
        val sc = new SparkContext(conf)
        val chekPath = propertie.getProperty("chekPath")
        println(chekPath)
        //创建topic
        val topic = propertie.getProperty("topic")
        //配置kafka集群信息
        val brokers = propertie.getProperty("brokers")
        val property = new Properties()
        property.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
        property.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        property.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
//        property.put("security.protocol", "SASL_PLAINTEXT")
//        property.put("sasl.kerberos.service.name", "kafka")
        property.put("client.id", "dataProducer")
        //原始数据文件路径
        val inputPath = propertie.getProperty("inputPath")
        println("inputPath:" + inputPath)
        //读取原始数据
        val file = sc.textFile(inputPath)
        file.foreachPartition((partisions: Iterator[String]) => {
            //创建生产者
            val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](property)
            partisions.foreach(x => {
                val message: ProducerRecord[String, String] = new ProducerRecord[String, String](topic.toString, x.toString)
                println(x.toString)
                //生产者将消息写入kafka
                try {
                    producer.send(message)
                    println("producer completed.")
                }
                catch {
                    case e: Exception => {
                        println("Task not serialized.")
                    }
                }
            })
            producer.close()
        })
    }
}
