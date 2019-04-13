package com.huawei.bigdata.spark.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * create table in hbase.
 */
public class TableCreation {
  public static void main(String[] args) throws IOException {


    //SparkConf对象用于设置spark的配置项。SetAppName是为这个应用程序的名字。
    SparkConf conf = new SparkConf().setAppName("CollectFemaleInfo");
    //通过设置创建一个操作Spark的对象
    JavaSparkContext jsc = new JavaSparkContext(conf);
    // 创建配置参数以连接HBase。 hbase-site.xml必须包含在类路径中
    Configuration hbConf = HBaseConfiguration.create(jsc.hadoopConfiguration());
    // 创建连接通道以连接HBase。
    Connection connection = ConnectionFactory.createConnection(hbConf);
    //声明表的描述。
    TableName userTable = TableName.valueOf("table2");
    //构造一个指定TableName对象的表描述符
    HTableDescriptor tableDescr = new HTableDescriptor(userTable);
    //添加列族。
    tableDescr.addFamily(new HColumnDescriptor("info".getBytes()));

    // Create a table.
    System.out.println("Creating table table. ");
    //HBase的管理API。从中获取实例Connection.getAdmin()并close()在完成后调用。
    //Admin可用于创建，删除，列出，启用和禁用以及以其他方式修改表，以及执行其他管理操作。
    //检索Admin实施以管理HBase集群。
    Admin admin = connection.getAdmin();
    if (admin.tableExists(userTable)) {
      admin.disableTable(userTable);
      admin.deleteTable(userTable);
    }
    admin.createTable(tableDescr);

    connection.close();
    jsc.stop();
    System.out.println("Done!");
  }
}
