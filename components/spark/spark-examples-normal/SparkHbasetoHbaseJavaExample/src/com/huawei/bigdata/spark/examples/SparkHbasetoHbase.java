package com.huawei.bigdata.spark.examples;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import scala.Tuple2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.*;


/**
 * calculate data from hbase1/hbase2,then update to hbase2
 */
public class SparkHbasetoHbase {

  public static void main(final String[] args) throws Exception {

    //用于启动Spark的配置，一些设置可以在这里进行设置，比如Appname为应用的名字
    SparkConf conf = new SparkConf().setAppName("SparkHbasetoHbase");
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");//序列化
    conf.set("spark.kryo.registrator", "com.huawei.bigdata.spark.examples.MyRegistrator");

    //通过配置文件创建一个操作Spark的对象
    JavaSparkContext jsc = new JavaSparkContext(conf);

    // 建立连接hbase的配置参数，此时需要保证hbase-site.xml在classpath中
    Configuration hbConf = HBaseConfiguration.create(jsc.hadoopConfiguration());

    // Scan为Hbase操作表的对象
    Scan scan = new org.apache.hadoop.hbase.client.Scan();

    //声明要查询的表的信息。
    scan.addFamily(Bytes.toBytes("cf"));//列族

    //反序列化，将扫描类转化成字符串
    org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
   // 转成Base64编码
    String scanToString = Base64.encodeBytes(proto.toByteArray());
    //TableInputFormat将HBase表格数据转换为MAP/reduce可以使用的格式
    hbConf.set(TableInputFormat.INPUT_TABLE, "table1");//table name
    //Base-64����ɨ���ǡ�
    hbConf.set(TableInputFormat.SCAN, scanToString);

    //通过spark接口获取表中的数据
    //读取数据并转化成rdd TableInputFormat 是 org.apache.hadoop.hbase.mapreduce 包下的。获得hbase查询结果Result
    JavaPairRDD rdd = jsc.newAPIHadoopRDD(hbConf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

    // 遍历hbase table1表中的每一个partition, 然后更新到Hbase table2表
    // 如果数据条数较少，也可以使用rdd.foreach()方法
    rdd.foreachPartition(//迭代器
      new VoidFunction<Iterator<Tuple2<ImmutableBytesWritable, Result>>>() {
		  //Tuple2相当于一个容器，里面存放的是查询处理的结果，结果的类型可能不一致。可以通过_1(),_2()来进行调用
        public void call(Iterator<Tuple2<ImmutableBytesWritable, Result>> iterator) throws Exception {
          hBaseWriter(iterator);//调用写入的方法
        }
      }
    );

    jsc.stop();
  }

  /**
   * write to table2 in exetutor
   *
   * @param iterator partition data from table1
   */
  private static void hBaseWriter(Iterator<Tuple2<ImmutableBytesWritable, Result>> iterator) throws IOException {
    //read hbase
    String tableName = "table2";
    String columnFamily = "cf";
    String qualifier = "cid";
    Configuration conf = HBaseConfiguration.create();//使用HBase资源创建配置
    Connection connection = null;
    Table table = null;
    try {
      connection = ConnectionFactory.createConnection(conf);//根据配置创建一个Connection对象
      table = connection.getTable(TableName.valueOf(tableName));//检索用于访问表的Table实现

      List<Get> rowList = new ArrayList<Get>();
      List<Tuple2<ImmutableBytesWritable, Result>> table1List = new ArrayList<Tuple2<ImmutableBytesWritable, Result>>();
	  //进行迭代，把从table1的数据放到list中。
      while (iterator.hasNext()) {
        Tuple2<ImmutableBytesWritable, Result> item = iterator.next();
        Get get = new Get(item._2().getRow());//获得数据的第二个值，resulet对应的值ֵ
        table1List.add(item);
        rowList.add(get);
      }

      //从table2中获得数据
      Result[] resultDataBuffer = table.get(rowList);
      List<Put> putList = new ArrayList<Put>();
      for (int i = 0; i < resultDataBuffer.length; i++) {
		//遍历table2的每条数据
        Result resultData = resultDataBuffer[i]; 
		//判空
        if (!resultData.isEmpty()) {
          //query hbase1Value
          String hbase1Value = "";
		  //将从table1取出来的数据放到迭代器中，进行遍历
          Iterator<Cell> it = table1List.get(i)._2().listCells().iterator();
          while (it.hasNext()) {
            Cell c = it.next();
            // 通过列族和修饰符去获得table1的值
            if (columnFamily.equals(Bytes.toString(CellUtil.cloneFamily(c)))
              && qualifier.equals(Bytes.toString(CellUtil.cloneQualifier(c)))) {
              hbase1Value = Bytes.toString(CellUtil.cloneValue(c));
            }
          }

          String hbase2Value = Bytes.toString(resultData.getValue(columnFamily.getBytes(), qualifier.getBytes()));
		  //对象进行put操作，必须首先实例化put。
          Put put = new Put(table1List.get(i)._2().getRow());

          //计算结果值
          int resultValue = Integer.parseInt(hbase1Value) + Integer.parseInt(hbase2Value);
          //设置数据
          put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(String.valueOf(resultValue)));
          putList.add(put);
        }
      }

      if (putList.size() > 0) {
        table.put(putList);
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (table != null) {
        try {
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
