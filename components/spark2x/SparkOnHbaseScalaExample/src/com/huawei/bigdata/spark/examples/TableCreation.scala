package com.huawei.bigdata.spark.examples

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
  * create table in hbase.
  */
object TableCreation {
  def main(args: Array[String]): Unit = {
    // Create the configuration parameter to connect the HBase. The hbase-site.xml must be included in the classpath.
    val conf: SparkConf = new SparkConf
    val sc: SparkContext = new SparkContext(conf)
    val hbConf: Configuration = HBaseConfiguration.create(sc.hadoopConfiguration)

    // Create the connection channel to connect the HBase
    val connection: Connection = ConnectionFactory.createConnection(hbConf)

    // Declare the description of the table
    val userTable = TableName.valueOf("shb1")
    val tableDescr = new HTableDescriptor(userTable)
    tableDescr.addFamily(new HColumnDescriptor("info".getBytes))

    // Create a table
    println("Creating table shb1. ")
    val admin = connection.getAdmin
    if (admin.tableExists(userTable)) {
      admin.disableTable(userTable)
      admin.deleteTable(userTable)
    }
    admin.createTable(tableDescr)

    connection.close()
    sc.stop()
    println("Done!")
  }
}
