package com.huawei.bigdata.spark.examples;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
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
import com.huawei.bigdata.spark.security.LoginUtil;


/**
 * calculate data from hbase1/hbase2,then update to hbase2
 */
public class SparkHbasetoHbase
{

    public static void main(String[] args) throws Exception
    {
        /******* 以上是认证代码************/
//        SparkConf conf = new SparkConf().setAppName("SparkHbasetoHbase");
//        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
//        conf.set("spark.kryo.registrator", "com.huawei.bigdata.spark.examples.MyRegistrator");
//        JavaSparkContext jsc = new JavaSparkContext(conf);
        SparkConf sparkConf = new SparkConf().setAppName("CollectPersonInfo").setMaster("local[2]");
        sparkConf.set("spark.testing.memory", "2147480000");
        //Spark使用Kryo序列化,减少内存的消耗,提高速度   1.开启Kryo序列化，2.class注册
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConf.set("spark.kryo.registrator", "com.huawei.bigdata.esandhbase.example.MyRegistrator");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        // Create the configuration parameter to connect the HBase. The hbase-site.xml must be included in the classpath.
        Configuration hbConf = HBaseConfiguration.create(jsc.hadoopConfiguration());
        hbConf.addResource(new Path(SparkHbasetoHbase.class.getClassLoader().getResource("core-site.xml").getPath()), false);
        hbConf.addResource(new Path(SparkHbasetoHbase.class.getClassLoader().getResource("hdfs-site.xml").getPath()), false);
        hbConf.addResource(new Path(SparkHbasetoHbase.class.getClassLoader().getResource("hbase-site.xml").getPath()), false);
        String krb5Conf =   SparkHbasetoHbase.class.getClassLoader().getResource("krb5.conf").getPath();
        String keyTab =  SparkHbasetoHbase.class.getClassLoader().getResource("user.keytab").getPath();
        String principal = "lyysxg";
        LoginUtil.setJaasFile(principal, keyTab);
        LoginUtil.setZookeeperServerPrincipal("zookeeper/hadoop.hadoop.com");
        LoginUtil.login(principal, keyTab, krb5Conf, hbConf);

        // Declare the information of the table to be queried.
        Scan scan = new org.apache.hadoop.hbase.client.Scan();
        scan.addFamily(Bytes.toBytes("info"));//colomn family
        org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
        String scanToString = Base64.encodeBytes(proto.toByteArray());
        hbConf.set(TableInputFormat.INPUT_TABLE, "table1");//table name
        hbConf.set(TableInputFormat.SCAN, scanToString);
        JavaPairRDD rdd = jsc.newAPIHadoopRDD(hbConf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

        // Traverse every Partition in the HBase table1 and update the HBase table2
        //If less data, you can use rdd.foreach()
        rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<ImmutableBytesWritable, Result>>>()
        {
            public void call(Iterator<Tuple2<ImmutableBytesWritable, Result>> iterator) throws Exception
            {
                hBaseWriter(iterator);
            }
        });

        jsc.stop();
    }

    /**
     * write to table2 in exetutor
     *
     * @param iterator partition data from table1
     */
    private static void hBaseWriter(Iterator<Tuple2<ImmutableBytesWritable, Result>> iterator) throws IOException
    {
        //read hbase
        String tableName = "table2";
        String columnFamily = "info";
        String qualifier = "上网时长";
        Configuration conf = HBaseConfiguration.create();
        Connection connection = null;
        Table table = null;

        try
        {
            connection = ConnectionFactory.createConnection(conf);
            table = connection.getTable(TableName.valueOf(tableName));

            List<Get> rowList = new ArrayList<Get>();
            List<Tuple2<ImmutableBytesWritable, Result>> table1List = new ArrayList<Tuple2<ImmutableBytesWritable, Result>>();
            while (iterator.hasNext())
            {
                Tuple2<ImmutableBytesWritable, Result> item = iterator.next();
                Get get = new Get(item._2().getRow());
                table1List.add(item);
                rowList.add(get);
            }

            //get data from hbase table2
            Result[] resultDataBuffer = table.get(rowList);

            //set data for hbase
            List<Put> putList = new ArrayList<Put>();
            for (int i = 0; i < resultDataBuffer.length; i++)
            {
                Result resultData = resultDataBuffer[i];//hbase2 row
                if (!resultData.isEmpty())
                {
                    //query hbase1Value
                    String hbase1Value = "";
                    Iterator<Cell> it = table1List.get(i)._2().listCells().iterator();
                    while (it.hasNext())
                    {
                        Cell c = it.next();
                        // query table1 value by colomn family and colomn qualifier
                        if (columnFamily.equals(Bytes.toString(CellUtil.cloneFamily(c))) && qualifier.equals(Bytes.toString(CellUtil.cloneQualifier(c))))
                        {
                            hbase1Value = Bytes.toString(CellUtil.cloneValue(c));
                        }
                    }

                    String hbase2Value = Bytes.toString(resultData.getValue(columnFamily.getBytes(), qualifier.getBytes()));
                    Put put = new Put(table1List.get(i)._2().getRow());

                    //calculate result value
                    int resultValue = Integer.parseInt(hbase1Value) + Integer.parseInt(hbase2Value);
                    //set data to put
                    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(String.valueOf(resultValue)));
                    putList.add(put);
                }
            }

            if (putList.size() > 0)
            {
                table.put(putList);
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        finally
        {
            if (table != null)
            {
                try
                {
                    table.close();
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                }
            }
            if (connection != null)
            {
                try
                {
                    // Close the HBase connection.
                    connection.close();
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                }
            }
        }
    }
}
