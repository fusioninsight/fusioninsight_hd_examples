package com.huawei.bigdata.kafkaTospark.HDFSToSpark;

import com.huawei.bigdata.kafkaTospark.LoginUtil.LoginUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;


import java.io.FileInputStream;
import java.util.Properties;

import static java.lang.Thread.sleep;


public class HdfsToSpark {
    static
    {
        PropertyConfigurator.configure(HdfsToSpark.class.getClassLoader().getResource("log4j.properties").getPath());
    }
    private final static Log LOG = LogFactory.getLog(HdfsToSpark.class);
    public static void main(String[] args) throws Exception,Throwable
    {
        //用于认证
        //打包放到服务器上运行，请删除
        UserLogin();

        //创建一个对象用于操作Spark
        SparkSession spark = SparkSession.builder().master("local").appName("spark core").getOrCreate();
        //加载测试文件，路径为HDFS上的路径
        Dataset data = spark.read().textFile("/zwl/KafkaToSparkSql/ceshi.txt");
        //将数据转换成RDD。
        JavaRDD<String> rdd =data.javaRDD();
        //用于加载kafka消费者的配置文件
        Properties property = new Properties();
        property.load(new FileInputStream(HdfsToSpark.class.getClassLoader().getResource("producer.properties").getPath()));
        property.put("acks", "all"); //服务端多少副本保存成功后，返回响应。0：不需要server相应，1：一个副本，all：所有副本
        property.put("retries", 1); //消息发送失败后，重试的次数
        property.put("batch.size", 16384); //向同一个partition批量发送消息的阈值，达到这个值后就会发送
        property.put("linger.ms", 10);//每次发送消息的时间间隔
        property.put("buffer.memory", 33554432);//保存还未来得及发送到服务端的消息的缓存大小。
        //发送时间间隔、partition数量、每个partition批量发送的阈值、缓存大小四个参数需要结合硬件配置、系统响应时延、带宽占用统筹考虑。
        property.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); //根据数据类型确定序列化方式
        property.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); //根据数据类型确定序列化方式
        //根据配置文件，创建一个生产者
        KafkaProducer<String,String> producer = new KafkaProducer<String,String>(property);
        //遍历数据，将数据到kafka上
        for(String t: rdd.collect()){
           producer.send(new ProducerRecord<String, String>("test-topic","inputdata", t));
            try {sleep(1000);} catch (Exception e) {LOG.error(e.toString());}
            LOG.info("The Producer have send {} messages"+t);
        }
    spark.stop();
    }
    public static void UserLogin() throws Exception{
        Configuration conf = new Configuration();
        conf.addResource(new Path(HdfsToSpark.class.getClassLoader().getResource("core-site.xml").getPath()));
        conf.addResource(new Path(HdfsToSpark.class.getClassLoader().getResource("hdfs-site.xml").getPath()));

        if ("kerberos".equalsIgnoreCase(conf.get("hadoop.security.authentication")))
        {
            //认证相关，安全模式需要，普通模式可以删除
            String PRNCIPAL_NAME = "lyysxg";//需要修改为实际在manager添加的用户
            String KRB5_CONF = HdfsToSpark.class.getClassLoader().getResource("krb5.conf").getPath();
            String KEY_TAB = HdfsToSpark.class.getClassLoader().getResource("user.keytab").getPath();
            System.setProperty("java.security.krb5.conf", KRB5_CONF); //指定kerberos配置文件到JVM
            LoginUtil.setJaasFile("lyysxg",KEY_TAB);
            LoginUtil.setKrb5Config(KRB5_CONF);
            LoginUtil.setZookeeperServerPrincipal("zookeeper/hadoop.hadoop.com");
            LoginUtil.login(PRNCIPAL_NAME,KEY_TAB,KRB5_CONF,conf);
        }
    }
}
