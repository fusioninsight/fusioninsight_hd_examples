package main.scala.com.zr.police

import java.io.{File, FileInputStream}
import java.util.Properties

import main.scala.com.zr.utils.LoginUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor}
import org.apache.hadoop.hbase.client.{HBaseAdmin, HTable, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object KafkaStreaming {
  def main(args: Array[String]): Unit = {
    //读取配置文件
    val propertie = new Properties()
    val in = this.getClass.getClassLoader().getResourceAsStream("police.properties")
    propertie.load(in)
    //进行安全认证
    val userPrincipal = propertie.getProperty("userPrincipal")
    val userKeytabPath = propertie.getProperty("userKeytabPath")
    val krb5ConfPath = propertie.getProperty("krb5ConfPath")
    val hadoopConf: Configuration = new Configuration()
    LoginUtil.login(userPrincipal, userKeytabPath, krb5ConfPath, hadoopConf)
    //spark配置信息
    val conf = new SparkConf().setMaster("local[2]").setAppName("kafka")
    val ssc = new StreamingContext(conf, Seconds(2))
    ssc.checkpoint(propertie.getProperty("chekPath"))

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.property.clientPort", propertie.getProperty("port"))
    hbaseConf.set("hbase.zookeeper.quorum", propertie.getProperty("zkquorum"))
    val tableName = propertie.getProperty("tableName")
        hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName)
    val jobConf = new JobConf(hbaseConf)
//    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,tableName)
    val topics = propertie.getProperty("topic").split("\\|").toSet
    val groupId = propertie.getProperty("groupId")
    val brokers = propertie.getProperty("brokers")
    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> brokers,
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> groupId
    )
    /**
      * 酒店住宿：姓名、身份证ID、年龄、性别、入住酒店地址、入住时间、退房时间、同行人。
      * 网吧上网：姓名、身份证ID、年龄、性别、网吧地址、上网日期、时长
      * 卡口身份核验：姓名、身份证ID、年龄、性别、卡口位置、核验时间、出行方式（自驾、乘车、步行）
      */
    val locationStrategy = LocationStrategies.PreferConsistent
    val consumerStrategies = ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    val kafkaStream = KafkaUtils.createDirectStream[String, String](ssc, locationStrategy, consumerStrategies)
    val cloudSolrClient = SolrSearch.getCloudSolrClient()
    val hBaseAdmin = new HBaseAdmin(hbaseConf)

    //创建hbase表
    if (!hBaseAdmin.isTableAvailable(tableName)) {
      val tableDesc = new HTableDescriptor(tableName)
      tableDesc.addFamily(new HColumnDescriptor(Bytes.toBytes("Basic")))
      tableDesc.addFamily(new HColumnDescriptor(Bytes.toBytes("OtherInfo")))
      hBaseAdmin.createTable(tableDesc)
    } else {
      print("Table " + tableName + " exists")
    }
    //索引名称
    val collectionName = propertie.getProperty("COLLECTION_NAME")
    //hbase表

    //查询所有索引名称
    val collectionNames = SolrSearch.queryAllCollections(cloudSolrClient)
    //判断要创建的索引名称是否已经存在,不存在则创建
    if (!collectionNames.contains(collectionName)) {
      SolrSearch.createCollection(collectionName)
    }
    //设置默认索引
    cloudSolrClient.setDefaultCollection(collectionName)
    kafkaStream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        rdd.map(x => (x.key(), x.value())).map(s => {
          val split = s._2.split("\\|")
          var put = new Put(Bytes.toBytes(split(1)))
          //将数据添加至hbase库
          put.addColumn(Bytes.toBytes("Basic"), Bytes.toBytes("name"), Bytes.toBytes(split(0)))
          put.addColumn(Bytes.toBytes("Basic"), Bytes.toBytes("age"), Bytes.toBytes(split(2)))
          put.addColumn(Bytes.toBytes("Basic"), Bytes.toBytes("name"), Bytes.toBytes(split(3)))
          put.addColumn(Bytes.toBytes("Basic"), Bytes.toBytes("sex"), Bytes.toBytes(split(4)))
          if (s._1.equals("hotel")) {
            //添加索引(姓名,地址,时间)
            SolrSearch.addDocs(cloudSolrClient, split(0), split(5), split(6), split(1))
            //将数据添加至hbase库
            put.addColumn(Bytes.toBytes("OtherInfo"), Bytes.toBytes("hotelAddr"), Bytes.toBytes(split(5)))
            put.addColumn(Bytes.toBytes("OtherInfo"), Bytes.toBytes("checkInTime"), Bytes.toBytes(split(6)))
            put.addColumn(Bytes.toBytes("OtherInfo"), Bytes.toBytes("checkOutTime"), Bytes.toBytes(split(7)))
            put.addColumn(Bytes.toBytes("OtherInfo"), Bytes.toBytes("acquaintance"), Bytes.toBytes(split(8)))
            (new ImmutableBytesWritable, put)
          } else if (s._1.equals("internet")) {
            //添加索引(姓名,地址,时间)
            SolrSearch.addDocs(cloudSolrClient, split(0), split(5), split(6), split(1))
            //将数据添加至hbase库
            put.addColumn(Bytes.toBytes("OtherInfo"), Bytes.toBytes("barAddr"), Bytes.toBytes(split(5)))
            put.addColumn(Bytes.toBytes("OtherInfo"), Bytes.toBytes("internetDate"), Bytes.toBytes(split(6)))
            put.addColumn(Bytes.toBytes("OtherInfo"), Bytes.toBytes("timeSpent"), Bytes.toBytes(split(7)))
            (new ImmutableBytesWritable, put)
          } else {
            //添加索引(姓名,地址,时间)
            SolrSearch.addDocs(cloudSolrClient, split(0), split(5), split(6), split(1))
            //将数据添加至hbase库
            put.addColumn(Bytes.toBytes("OtherInfo"), Bytes.toBytes("bayonetAddr"), Bytes.toBytes(split(5)))
            put.addColumn(Bytes.toBytes("OtherInfo"), Bytes.toBytes("checkDate"), Bytes.toBytes(split(6)))
            put.addColumn(Bytes.toBytes("OtherInfo"), Bytes.toBytes("tripType"), Bytes.toBytes(split(7)))
            (new ImmutableBytesWritable, put)
          }
        }).saveAsHadoopDataset(jobConf)
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }

}
