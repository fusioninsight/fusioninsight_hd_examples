package com.huawei.bigdata.spark.examples;

import java.io.IOException;
import java.util.List;

import scala.Tuple3;
import scala.Tuple4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 * load data into hbase table.
 */
public class TableInputData {
  public static void main(String[] args) throws IOException {

    SparkConf conf = new SparkConf().setAppName("CollectFemaleInfo");
    JavaSparkContext jsc = new JavaSparkContext(conf);
    // Create the configuration parameter to connect the HBase.
    Configuration hbConf = HBaseConfiguration.create(jsc.hadoopConfiguration());

    // Declare the information of the table.
    Table table = null;
    String tableName = "table2";
    byte[] familyName = Bytes.toBytes("info");
    Connection connection = null;

    try {
      // Connect to the HBase.
      connection = ConnectionFactory.createConnection(hbConf);
      // Obtain the table object.
      table = connection.getTable(TableName.valueOf(tableName));
      List<Tuple3<String, String, String>> data =
          jsc.textFile(args[0]).map(new Function<String, Tuple3<String, String, String>>() {
            public Tuple3<String, String, String> call(String s) throws Exception {
              String[] tokens = s.split(",");

              return new Tuple3<String, String, String>(tokens[0], tokens[1], tokens[2]);
            }
          }).collect();

      Integer i = 0;
      //插入数据
      for (Tuple3<String, String,String> line : data) {
        Put put = new Put(Bytes.toBytes("row" + i));
        put.addColumn(familyName, Bytes.toBytes("姓名"), Bytes.toBytes(line._1()));
        put.addColumn(familyName, Bytes.toBytes("性别"), Bytes.toBytes(line._2()));
        put.addColumn(familyName, Bytes.toBytes("上网时长"), Bytes.toBytes(line._3()));
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

      jsc.stop();
    }
  }
}
