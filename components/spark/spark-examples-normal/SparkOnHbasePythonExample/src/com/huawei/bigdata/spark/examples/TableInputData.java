package com.huawei.bigdata.spark.examples;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;

/**
 * Input data to hbase table.
 */
public class TableInputData {

  public void writetable(JavaRDD<List<String>> jRDD) throws IOException {


    // Create the configuration parameter to connect the HBase.
    Configuration hbConf = HBaseConfiguration.create();

    // Declare the information of the table.
    Table table = null;
    String tableName = "shb1";
    byte[] familyName = Bytes.toBytes("info");
    Connection connection = null;

    try {
      // Connect to the HBase.
      connection = ConnectionFactory.createConnection(hbConf);
      // Obtain the table object.
      table = connection.getTable(TableName.valueOf(tableName));

      List<List<String>> data = jRDD.collect();
      Integer i = 0;
      for (List<String> line : data) {
        Put put = new Put(Bytes.toBytes("row" + i));
        put.addColumn(familyName, Bytes.toBytes("c11"), Bytes.toBytes(line.get(0)));
        put.addColumn(familyName, Bytes.toBytes("c12"), Bytes.toBytes(line.get(1)));
        put.addColumn(familyName, Bytes.toBytes("c13"), Bytes.toBytes(line.get(2)));
        put.addColumn(familyName, Bytes.toBytes("c14"), Bytes.toBytes(line.get(3)));
        i += 1;
        table.put(put);
      }

    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (table != null) {
        try {
          // Close the HTable.
          table.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
      if (connection != null) {
        try {
          // Close the HBase connection.
          connection.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }
}
