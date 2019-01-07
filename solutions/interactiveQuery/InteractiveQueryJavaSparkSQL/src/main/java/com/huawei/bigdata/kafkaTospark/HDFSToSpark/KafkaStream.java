package com.huawei.bigdata.kafkaTospark.HDFSToSpark;

import com.huawei.bigdata.kafkaTospark.LoginUtil.AccountInfoBody;

import com.huawei.bigdata.kafkaTospark.LoginUtil.MoneyLaundInfoBody;
import com.huawei.bigdata.kafkaTospark.LoginUtil.TransactionInfoBody;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import scala.Tuple10;


import java.io.FileInputStream;
import java.util.*;

public class KafkaStream {
    //    static
//    {
//        PropertyConfigurator.configure(HdfsToSpark.class.getClassLoader().getResource("log4j-executor.properties").getPath());
//    }
    private final static Log LOG = LogFactory.getLog(HdfsToSpark.class.getName());
    private static SparkSession spark = null;
    private static Dataset<Row> dataset = null;

    public static void main(String[] args) throws Exception {
        //认证代码
        //在服务器上运行，请删除
        HdfsToSpark.UserLogin();

        //创建一个对象，用于加载消费者的配置文件。
        Properties properties = new Properties();
        properties.load(new FileInputStream(KafkaStream.class.getClassLoader().getResource("consumer.properties").getPath()));
        //集群消息
        String brokers = "189.211.68.235:21007,189.211.68.223:21007,189.211.69.32:21007";
        //kafka配置信息
        String topics = "test-topic";
        String batchTime = "3";
        String groupId = "testGroup";
        //创建Spark连接对象。
        SparkConf sparkConf = new SparkConf().setAppName("KafkaToESAndSparkStream").setMaster("local[2]");
        //通过
        JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, new Duration(Long.parseLong(batchTime) * 1000));

        String[] topicArr = topics.split(",");
        Set<String> topicSet = new HashSet<String>(Arrays.asList(topicArr));
        Map<String, Object> kafkaParams = new HashMap();
        kafkaParams.put("bootstrap.servers", brokers);
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("group.id", groupId);
        // kafkaParams.put("enable.auto.commit", "true");
        // kafkaParams.put("auto.commit.interval.ms", "100"); //offset自动提交间隔
        // 配置kafka consumer的认证。运行kafkaUtils.createDirectStream代码在认证后才能消费数据
        kafkaParams.put("security.protocol", "SASL_PLAINTEXT");
        kafkaParams.put("sasl.kerberos.service.name", "kafka");

        //设置PreferConsistent方式，均匀分配分区。
        LocationStrategy locationStrategy = LocationStrategies.PreferConsistent();
        //对consumer进行自定义配置。Subscribe方法提交参数列表和kafka参数的处理。
        ConsumerStrategy consumerStrategy = ConsumerStrategies.Subscribe(topicSet, kafkaParams);
        //建立kafka与sparkStreaming的连接，消费信息
        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(jsc, locationStrategy, consumerStrategy);
        //获得信息的value值。它是以key-value存储的，value存储我们的信息
        JavaDStream<String> lines = messages.map(new Function<ConsumerRecord<String, String>, String>() {
            @Override
            public String call(ConsumerRecord<String, String> tuple2) {
                return tuple2.value();
            }
        });
        //过滤，过滤掉空的消息
        JavaDStream<String> filter = lines.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                Boolean t = !s.equalsIgnoreCase("") || s.length() > 1;
                return t;
            }
        });
        //创建表
        CreateTable(filter);
        //启动应用
        jsc.start();
        //阻塞等待
        jsc.awaitTermination();
        Dataset<Row> result =spark.sql("select * from Acounct");
        List<String> results = result.javaRDD().map(new Function<Row, String>()
        {
            @Override
            public String call(Row row)
            {
                return row.getString(0) + "," + row.getLong(1);
            }
        }).collect();

        //服务停止
        spark.stop();
    }

    private static void CreateTable(JavaDStream<String> filter) {
        filter.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            @Override
            public void call(JavaRDD<String> stringJavaRDD) throws Exception {

                JavaRDD<Tuple10<String, String, String, String, String, String, String, String, String, String>> tuple = stringJavaRDD.map(new Function<String, Tuple10<String, String, String, String, String, String, String, String, String, String>>() {
                    public Tuple10<String, String, String, String, String, String, String, String, String, String> call(String s) throws Exception {
                        String[] data = s.split("\\|");
                        Tuple10<String, String, String, String, String, String, String, String, String, String> tuple = new Tuple10<String, String, String, String, String, String, String, String, String, String>(data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8], data[9]);
                        return tuple;
                    }
                });
                if (tuple.count()>0)
                {
                    JavaRDD<AccountInfoBody> accountInfo = tuple.map(new Function<Tuple10<String, String, String, String, String, String, String, String, String, String>, AccountInfoBody>() {
                        @Override
                        public AccountInfoBody call(Tuple10<String, String, String, String, String, String, String, String, String, String> s) throws Exception {
                            AccountInfoBody accountInfoBody = new AccountInfoBody();
                            accountInfoBody.setIDCard(s._1());
                            accountInfoBody.setBankAccount(s._2());
                            accountInfoBody.setAccountName(s._3());
                            return accountInfoBody;
                        }
                    });
                    JavaRDD<TransactionInfoBody> transactionInfo = tuple.map(new Function<Tuple10<String, String, String, String, String, String, String, String, String, String>, TransactionInfoBody>()
                    {
                        @Override
                        public TransactionInfoBody call(Tuple10<String, String, String, String, String, String, String, String, String, String> s) throws Exception
                        {
                            TransactionInfoBody transactionInfoBody = new TransactionInfoBody();
                            transactionInfoBody.setBankAcount(s._1());
                            transactionInfoBody.setTransDate(s._4());
                            transactionInfoBody.setTransMoney(s._5());
                            transactionInfoBody.setTransType(s._6());
                            transactionInfoBody.setTargetAcount(s._7());
                            transactionInfoBody.setRemarks(s._10());
                            return transactionInfoBody;
                        }
                    }
                    );
                    JavaRDD<MoneyLaundInfoBody> moneyLaundInfo = tuple.map(new Function<Tuple10<String, String, String, String, String, String, String, String, String, String>, MoneyLaundInfoBody>()
                    {
                        @Override
                        public MoneyLaundInfoBody call(Tuple10<String, String, String, String, String, String, String, String, String, String> s) throws Exception
                        {
                            MoneyLaundInfoBody moneyLaundInfo = new MoneyLaundInfoBody();
                            moneyLaundInfo.setBankAcount(s._1());
                            moneyLaundInfo.setIdentDate(s._4());
                            return moneyLaundInfo;
                        }
                    });


                        spark = SparkSession.builder().appName("1").master("local").getOrCreate();
                        dataset = spark.createDataFrame(accountInfo, AccountInfoBody.class);
                        dataset = spark.createDataFrame(transactionInfo, TransactionInfoBody.class);
                        dataset = spark.createDataFrame(moneyLaundInfo, MoneyLaundInfoBody.class);
                        dataset.registerTempTable("AcountInfo");
                        dataset.registerTempTable("TransactionInfo");
                        dataset.registerTempTable("moneyLaundInfo");

                        dataset.cache();
                        dataset.show();
                    Dataset<Row> result =spark.sql("select * from AcountInfo");
                    List<String> results = result.javaRDD().map(new Function<Row, String>()
                    {
                        @Override
                        public String call(Row row)
                        {
                            return row.toString();
                        }
                    }).collect();
                    LOG.info("########## Collect Result Start ############");
                    LOG.info(results);
                    LOG.info("########## Collect Result End ##############");
                    LOG.warn("-------------------------------");
                }
            }
        });
    }

}
