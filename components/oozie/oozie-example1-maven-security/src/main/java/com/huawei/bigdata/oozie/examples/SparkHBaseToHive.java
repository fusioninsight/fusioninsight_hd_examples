package com.huawei.bigdata.oozie.examples;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.security.PrivilegedExceptionAction;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import com.huawei.bigdata.oozie.security.LoginUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;

import scala.Tuple2;

public class SparkHBaseToHive implements Serializable
{
    private static final Logger LOG = Logger.getLogger(SparkHBaseToHive.class);

    private static final String ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME = "Client";

    private static final String ZOOKEEPER_SERVER_PRINCIPAL_KEY = "zookeeper.server.principal";

    private static final String ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL = "zookeeper/hadoop";

    public static void main(final String[] args) throws Exception
    {
        System.out.println("login user is " + UserGroupInformation.getLoginUser());
        // 认证
        login(new Configuration());
        // 读写数据
        UserGroupInformation.getLoginUser().doAs(new PrivilegedExceptionAction<Void>()
        {
            public Void run() throws Exception
            {
                new SparkHBaseToHive().runJob();
                return null;
            }
        });
    }

    private static void login(Configuration conf) throws IOException
    {
        String userPrincipal = "ladmin";
        String userKeytabFile = "/tmp/conf/user.keytab";
        String krb5File = "/tmp/conf/krb5.conf";

        LoginUtil.setJaasConf(ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME, userPrincipal, userKeytabFile);
        LoginUtil.setZookeeperServerPrincipal(ZOOKEEPER_SERVER_PRINCIPAL_KEY, ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL);
        LoginUtil.login(userPrincipal, userKeytabFile, krb5File, conf);

        System.out.println("===========login success======================" + userKeytabFile + "--------" + krb5File);
    }

    private void runJob() throws Exception
    {
        SparkConf conf = new SparkConf().setAppName("SparkHBaseToHive");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "org.apache.oozie.examples.MyRegistrator");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        // Create the configuration parameter to connect the HBase. The
        // hbase-site.xml must be included in the classpath.
        Configuration hbConf = HBaseConfiguration.create(jsc.hadoopConfiguration());

        // 一定要加载这些-site.xml文件，文件从客户端的/opt/client/HBase/hbase/conf路径下获取
        try
        {
            String coresite = "/tmp/conf/core-site.xml";
            String hbasesite = "/tmp/conf/hbase-site.xml";
            String hdfssite = "/tmp/conf/hdfs-site.xml";
            hbConf.addResource(new File(coresite).toURI().toURL());
            hbConf.addResource(new File(hbasesite).toURI().toURL());
            hbConf.addResource(new File(hdfssite).toURI().toURL());
        }
        catch (Exception e)
        {
            e.printStackTrace(System.out);
        }

        // Declare the information of the table to be queried.
        Scan scan = new org.apache.hadoop.hbase.client.Scan();
        // 列簇
        scan.addFamily(Bytes.toBytes("f1"));// colomn family
        org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
        String scanToString = Base64.encodeBytes(proto.toByteArray());
        // 表名
        hbConf.set(TableInputFormat.INPUT_TABLE, "t3");// table name
        hbConf.set(TableInputFormat.SCAN, scanToString);

        HiveContext sqlContext = new org.apache.spark.sql.hive.HiveContext(jsc);
        // Obtain the data in the table through the Spark interface.
        JavaPairRDD<ImmutableBytesWritable, Result> rdd = jsc.newAPIHadoopRDD(hbConf, TableInputFormat.class,
                ImmutableBytesWritable.class, Result.class);

        JavaRDD<Person> mapRDD = rdd.map(new Function<Tuple2<ImmutableBytesWritable, Result>, Person>()
        {
            public Person call(Tuple2<ImmutableBytesWritable, Result> tuple) throws Exception
            {
                // read hbase
                Iterator<Cell> it = tuple._2().listCells().iterator();

                // 创建Hive表对应的实体Bean
                Person person = new Person();
                while (it.hasNext())
                {
                    Cell c = it.next();
                    // query table1 value by colomn family and
                    // colomn qualifier
                    person.setName(Bytes.toString(CellUtil.cloneRow(c)));
                    person.setAccount(Integer.parseInt(Bytes.toString(CellUtil.cloneValue(c))));
                }

                return person;
            }
        });

        // 将数据转化为DataFrame
        DataFrame dataFrame = sqlContext.createDataFrame(mapRDD, Person.class);
        // 将DataFrame以Overwrite的模式写入person表
        dataFrame.write().mode(SaveMode.Overwrite).saveAsTable("person");
        jsc.stop();
    }
}
