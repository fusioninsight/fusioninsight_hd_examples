package main.scala.com.zr.police

import java.io.{File, FileInputStream}
import java.util.Properties

import main.scala.com.zr.utils.LoginUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.filter.{ByteArrayComparable, CompareFilter, Filter, RowFilter}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}

object HbaseQuery {
  def queryInfo() {
    //读取配置文件
    val propertie = new Properties()
    val in = this.getClass.getClassLoader().getResourceAsStream("police.properties")
    //    val path = System.getProperty("user.dir") + File.separator + "main/resource" + File.separator + "Fproducer.properties"
    propertie.load(in)
    val userPrincipal = propertie.getProperty("userPrincipal")
    val userKeytabPath = propertie.getProperty("userKeytabPath")
    val krb5ConfPath = propertie.getProperty("krb5ConfPath")
    val hadoopConf: Configuration = new Configuration()
    LoginUtil.login(userPrincipal, userKeytabPath, krb5ConfPath, hadoopConf)
    val conf = new SparkConf().setMaster("local[2]").setAppName("hbase")
    val sc = new SparkContext(conf)
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf.set("hbase.zookeeper.quorum", "master")
    val jobConf = new JobConf(hbaseConf, this.getClass)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, "POLICE_INFO")
    val scan = new Scan()
    val queryCondition = propertie.getProperty("queryCondition")
    val filter = new RowFilter(CompareFilter.CompareOp.EQUAL, ByteArrayComparable.parseFrom(queryCondition.getBytes()))
    scan.setFilter(filter)
    val proto = ProtobufUtil.toScan(scan)
    val scanToString = Base64.encodeBytes(proto.toByteArray)
    hbaseConf.set(TableInputFormat.SCAN, scanToString)
    val rdd = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    rdd.cache()
    rdd.foreach(x=>{
      print(x.toString())
    })
  }
}
