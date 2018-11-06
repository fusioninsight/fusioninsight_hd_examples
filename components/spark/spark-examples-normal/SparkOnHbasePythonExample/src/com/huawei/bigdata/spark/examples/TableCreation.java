package com.huawei.bigdata.spark.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

/**
 * Create table in hbase.
 */
public class TableCreation {

  public void createtable() throws IOException {

    // Create the configuration parameter to connect the HBase. The hbase-site.xml must be included in the classpath
    Configuration hbConf = HBaseConfiguration.create();

    // Create the connection channel to connect the HBase
    Connection connection = ConnectionFactory.createConnection(hbConf);

    // Declare the description of the table.
    TableName userTable = TableName.valueOf("shb1");
    HTableDescriptor tableDescr = new HTableDescriptor(userTable);
    tableDescr.addFamily(new HColumnDescriptor("info".getBytes()));

    // Create a table.
    System.out.println("Creating table shb1. ");
    Admin admin = connection.getAdmin();
    if (admin.tableExists(userTable)) {
      admin.disableTable(userTable);
      admin.deleteTable(userTable);
    }
    admin.createTable(tableDescr);

    connection.close();
    System.out.println("Done!");
  }
}
