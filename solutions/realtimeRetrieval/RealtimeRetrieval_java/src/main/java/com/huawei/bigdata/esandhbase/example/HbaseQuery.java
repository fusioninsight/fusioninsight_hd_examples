package com.huawei.bigdata.esandhbase.example;

import com.huawei.bigdata.security.LoginUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.FileInputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

public class HbaseQuery {
    private static final String ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL = "zookeeper/hadoop.hadoop.com";
    private static Properties properties = new Properties();

    public static void main(String[] args) throws Exception {
        queryInfo();
    }
    public static void queryInfo() throws Exception {
        properties.load(new FileInputStream(HbaseQuery.class.getClassLoader().getResource("consumer.properties").getPath()));

        //加载spark配置
        SparkConf sparkConf = new SparkConf().setAppName("CollectPersonInfo").setMaster("local[2]");
        //Spark使用Kryo序列化,减少内存的消耗,提高速度   1.开启Kryo序列化，2.class注册
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConf.set("spark.kryo.registrator", "com.huawei.bigdata.esandhbase.example.MyRegistrator");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        Configuration hbConf = HBaseConfiguration.create(jsc.hadoopConfiguration());
        //加载HDFS/HBase服务端配置，用于客户端与服务端对接
        hbConf.addResource(new Path(HbaseQuery.class.getClassLoader().getResource("core-site.xml").getPath()), false);
        hbConf.addResource(new Path(HbaseQuery.class.getClassLoader().getResource("hdfs-site.xml").getPath()), false);
        hbConf.addResource(new Path(HbaseQuery.class.getClassLoader().getResource("hbase-site.xml").getPath()), false);

        //******认证 Start*******
        //安全模式需要，普通模式可以删除
        String krb5Conf =   HbaseQuery.class.getClassLoader().getResource("krb5.conf").getPath();
        String keyTab =  HbaseQuery.class.getClassLoader().getResource("user.keytab").getPath();
        String principal = "fwc";
        LoginUtil.setJaasFile(principal, keyTab);
        LoginUtil.setZookeeperServerPrincipal(ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL);
        LoginUtil.login(principal, keyTab, krb5Conf, hbConf);


        // 声明要查询的表的信息。
        Scan scan = new org.apache.hadoop.hbase.client.Scan();
        scan.addFamily(Bytes.toBytes("Basic"));
        scan.addFamily(Bytes.toBytes("OtherInfo"));
        String queryCondition = properties.getProperty("queryCondition");
        //RowFilter(行健过滤器)相关的过滤方法使用:
        //查询rowkey为queryCondition的信息
        RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,new SubstringComparator(queryCondition));
//        提取rowkey以01结尾数据
//        Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,new RegexStringComparator(".*01$"));
//        提取rowkey以123开头的数据
//        Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,new BinaryPrefixComparator("123".getBytes()));
        scan.setFilter(filter);
        org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
        String scanToString = Base64.encodeBytes(proto.toByteArray());
        hbConf.set(TableInputFormat.INPUT_TABLE, properties.getProperty("tableName"));
        hbConf.set(TableInputFormat.SCAN, scanToString);

        //通过Spark接口获取表中的数据。
        JavaPairRDD rdd = jsc.newAPIHadoopRDD(hbConf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
        List<Tuple2<ImmutableBytesWritable, Result>> rddList = rdd.collect();
        for (int i = 0; i < rddList.size(); i++) {
            Tuple2<ImmutableBytesWritable, Result> t2 = rddList.get(i);
            ImmutableBytesWritable key = t2._1();
            Iterator<Cell> it = t2._2().listCells().iterator();
            while (it.hasNext()) {
                Cell c = it.next();
                String family = Bytes.toString(CellUtil.cloneFamily(c));
                String qualifier = Bytes.toString(CellUtil.cloneQualifier(c));
                String value = Bytes.toString(CellUtil.cloneValue(c));
                Long tm = c.getTimestamp();
                System.out.println(" Family=" + family + " Qualifier=" + qualifier + " Value=" + value + " TimeStamp=" + tm);
            }
        }
        jsc.stop();
    }
}
