package com.huawei.hbase.client

import java.io.FileInputStream
import java.util.Properties

import org.apache.hadoop.hbase._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ArrayBuffer

object HbaseQueryByID {


    //加载hbase配置
    def getAdmin(): HBaseAdmin = {

      val property = new Properties()
      property.load(new FileInputStream("resources/ParamsConf.properties"))
      val myConf = HBaseConfiguration.create()
      val tableName = "ControlInfo"
      val zkQuorun = property.getProperty("zk.quorum")
      val zkPort = property.getProperty("zk.port")
      myConf.set("hbase.zookeeper.quorum", zkQuorun)
      myConf.set("hbase.zookeeper.property.clientPort", zkPort)
      myConf.set("hbase.defaults.for.version.skip", "true")
      val admin = new HBaseAdmin(myConf)
      return admin
    }

  def createTable():Unit = {
    val tableName = "ControlInfo"
    val admin = HbaseQueryByID.getAdmin()
    val tableDesc = new HTableDescriptor(TableName.valueOf(tableName))
    val hcd = new HColumnDescriptor()
  }

    //全表扫描
    def getByScan(conf: Configuration, tableName: String): ArrayBuffer[Array[Cell]] = {

      var arrBuffer = ArrayBuffer[Array[Cell]]()
      val scaner = new Scan()
      val table = ConnectionFactory.createConnection(conf).getTable(TableName.valueOf(tableName))
      val results = table.getScanner(scaner)
      var res: Result = results.next()
      while (res != null) {
        res = results.next()
      }
      arrBuffer
    }

    //根据id（row）进行查询
    def getByRowByID(conf: Configuration, tableName: String, row: String): Array[Cell] = {

      val table = ConnectionFactory.createConnection(conf).getTable(TableName.valueOf(tableName))

      val get = new Get(Bytes.toBytes(row))
      val res = table.get(get)
      res.rawCells()

    }
}
