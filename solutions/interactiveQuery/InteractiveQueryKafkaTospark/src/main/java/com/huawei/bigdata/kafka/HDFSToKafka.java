package com.huawei.bigdata.kafka;

import com.huawei.bigdata.LoginUtil.LoginUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import scala.Function1;
import scala.collection.Iterator;
import scala.runtime.BoxedUnit;

import java.io.FileInputStream;
import java.io.FilterInputStream;
import java.io.InputStream;
import java.util.Properties;

public class HDFSToKafka {
    static
    {
        PropertyConfigurator.configure(HDFSToKafka.class.getClassLoader().getResource("log4j.properties").getPath());
    }
     private final static Log LOG = LogFactory.getLog(HDFSToKafka.class.getName());

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        conf.addResource(new Path(HDFSToKafka.class.getClassLoader().getResource("core-site.xml").getPath()));
        conf.addResource(new Path(HDFSToKafka.class.getClassLoader().getResource("hdfs-site.xml").getPath()));
        if("kerberos".equalsIgnoreCase(conf.get("hadoop.security.authentication")))
        {
            String userName = "lyysxg";
            String KRB5_CONF = HDFSToKafka.class.getClassLoader().getResource("krb5.conf").getPath();
            String KEY_TAB =HDFSToKafka.class.getClassLoader().getResource("user.keytab").getPath();
            LoginUtil.setJaasFile("lyysxg",KEY_TAB);
            LoginUtil.setZookeeperServerPrincipal("zookeeper/hadoop.hadoop.com");
            LoginUtil.login(userName,KEY_TAB,KRB5_CONF,conf);
        }
       //原始数据路径
        String inputPath = "/qlz/test2/part-00000";
        System.out.println(inputPath);
        SparkSession spark = SparkSession.builder().appName("kafkaToSpark").master("local").getOrCreate();
        Dataset initialData = spark.read().textFile(inputPath);
        String brokers = "189.211.69.32:21005,189.211.68.223:21005,189.211.68.236:21005";
        Properties property = new Properties();
        property.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        property.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        property.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        property.put("client.id", "dataProducer");
        for(String t:initialData.collect() ){


        }
    }

}
