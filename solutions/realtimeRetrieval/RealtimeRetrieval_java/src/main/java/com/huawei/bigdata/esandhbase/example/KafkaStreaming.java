package com.huawei.bigdata.esandhbase.example;
import com.huawei.bigdata.security.LoginUtil;
import com.huawei.bigdata.security.LoginUtilForKafka;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.hadoop.hbase.client.*;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

import com.huawei.bigdata.security.LoginUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.util.Arrays;
import java.util.Properties;
/*
 * 执行producer之前，先参考shell/createTopic.sh创建topic，并配置权限。多个producer可以向一个topic写入。
 * 样例需要先启动KafkaStreaming，再启动DataProducer
 */
public class KafkaStreaming {
    static {
        PropertyConfigurator.configure(KafkaStreaming.class.getClassLoader().getResource("log4j.properties").getPath());
    }
    private static final Logger LOG = LoggerFactory.getLogger(KafkaStreaming.class);
    private static Properties properties = new Properties();

    private static TableName tableName = null;
    private static Connection conn = null;
    private static Admin admin = null;
    private static Table table = null;
    private static RestClient restClient = null;

    public static void main(String[] args)throws Exception {
        Configuration conf = HBaseConfiguration.create();
        //加载HDFS/HBase服务端配置，用于客户端与服务端对接
        conf.addResource(new Path(KafkaStreaming.class.getClassLoader().getResource("core-site.xml").getPath()));
        conf.addResource(new Path(KafkaStreaming.class.getClassLoader().getResource("hdfs-site.xml").getPath()));
        conf.addResource(new Path(KafkaStreaming.class.getClassLoader().getResource("hbase-site.xml").getPath()));
        //安全模式需要，普通模式可以删除s
        String krb5Conf =  KafkaStreaming.class.getClassLoader().getResource("krb5.conf").getPath();
        String keyTab = KafkaStreaming.class.getClassLoader().getResource("user.keytab").getPath();
        LoginUtilForKafka.setJaasFile("fwc", keyTab);
        LoginUtilForKafka.setKrb5Config(krb5Conf);
        LoginUtilForKafka.setZookeeperServerPrincipal("zookeeper/hadoop.hadoop.com");
        LoginUtil.login("fwc", keyTab, krb5Conf, conf);

        //加载consumer 配置 文件信息
        properties.load(new FileInputStream(KafkaStreaming.class.getClassLoader().getResource("consumer.properties").getPath()));
        String brokers = properties.getProperty("bootstrap.servers");
        String topics = properties.getProperty("topic");
        String batchTime = properties.getProperty("batchTime");
        String groupId = properties.getProperty("group.id");
        String indexName = properties.getProperty("indexName");

        //加载spark配置
        //创建一个Streaming启动环境。
        SparkConf sparkConf = new SparkConf().setAppName("KafkaToESAndHBase").setMaster("local[2]");
        sparkConf.set("spark.testing.memory", "2147480000");
        //streaming上下文，每隔多少秒执行一次获取批量数据然后处理这些数据
        JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, new Duration(Long.parseLong(batchTime) * 1000));
        //为Streaming配置CheckPoint目录。它将足够多的信息checkpoint到某些具备容错性的存储系统如hdfs上，以便出错时能够迅速恢复
        jsc.checkpoint(properties.getProperty("checkPath"));

        //获取kafka使用的主题列表
        String[] topicArr = topics.split(",");
        Set<String> topicSet = new HashSet<String>(Arrays.asList(topicArr));
        Map<String, Object> kafkaParams = new HashMap();
        kafkaParams.put("bootstrap.servers", brokers);
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("group.id", groupId);
        kafkaParams.put("enable.auto.commit", "true");
        kafkaParams.put("auto.commit.interval.ms", "100"); //offset自动提交间隔

        // 配置kafka consumer的认证。运行kafkaUtils.createDirectStream代码在认证后才能消费数据
        kafkaParams.put("security.protocol","SASL_PLAINTEXT");
        kafkaParams.put("sasl.kerberos.service.name","kafka");

        //设置PreferConsistent方式，均匀分配分区。
        LocationStrategy locationStrategy = LocationStrategies.PreferConsistent();
        //对consumer进行自定义配置。Subscribe方法提交参数列表和kafka参数的处理。
        ConsumerStrategy consumerStrategy = ConsumerStrategies.Subscribe(topicSet, kafkaParams);

        ESSearch.init();
        restClient = ESSearch.getRestClient();
        //判断要创建的索引名称是否已经存在,不存在则创建
        if (!ESSearch.exist(restClient,indexName)) {
            ESSearch.createIndex(indexName);
        }

        try
        {
            conn = ConnectionFactory.createConnection(conf);
        }
        catch (Exception e)
        {
            LOG.error("Failed to createConnection because ", e);
        }
        tableName = TableName.valueOf(properties.getProperty("tableName"));
        table = conn.getTable(tableName);
        try {
            admin = conn.getAdmin();
            if (!admin.tableExists(tableName))
            {
                LOG.info("Creating table...");
                HTableDescriptor htd = new HTableDescriptor(tableName);
                HColumnDescriptor hcd1 = new HColumnDescriptor("Basic");
                HColumnDescriptor hcd2 = new HColumnDescriptor("OtherInfo");
                hcd1.setCompressionType(Compression.Algorithm.SNAPPY);
                hcd1.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
                hcd2.setCompressionType(Compression.Algorithm.SNAPPY);
                hcd2.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
                htd.addFamily(hcd1);
                htd.addFamily(hcd2);
                // 指定起止RowKey和region个数；此时的起始RowKey为第一个region的endKey，结束key为最后一个region的startKey。
                admin.createTable(htd, Bytes.toBytes(100000),   Bytes.toBytes(400000), 10);
            }
            else
            {
                LOG.warn("table already exists");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        //从Kafka接收数据并生成相应的DStream( DStream操作最终会转换成底层的RDD的操作)
        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(jsc, locationStrategy, consumerStrategy);
        messages.foreachRDD(
            new VoidFunction<JavaRDD<ConsumerRecord<String, String>>>() {
                @Override
                public void call(JavaRDD<ConsumerRecord<String, String>> consumerRecordJavaRDD) throws Exception {
                    //使用 foreachPartition 方法，每个分区并发工作
                    consumerRecordJavaRDD.foreachPartition(
                        new VoidFunction<Iterator<ConsumerRecord<String, String>>>() {
                             @Override
                             public void call(Iterator<ConsumerRecord<String, String>> consumerRecordIterator) throws Exception {
                                  hbaseAndESWrite(consumerRecordIterator);
                             }
                        }
                     );
                }
            }
        );
        // Spark Streaming系统启动
        jsc.start();
        jsc.awaitTermination();

        if (table != null) {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (admin != null) {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (conn != null) {
            try {
                // Close the HBase connection.
                conn.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        //在进行完Elasticsearch操作后，需要调用“restClient.close()”关闭所申请的资源。
        if( restClient!=null) {
            try {
                restClient.close();
                LOG.info("Close the client successful in main.");
            } catch (Exception e1) {
                LOG.error("Close the client failed in main.",e1);
            }
        }
    };

    private static void hbaseAndESWrite(Iterator<ConsumerRecord<String, String>> consumerRecordIterator) throws IOException {
        try {
            List<Put> putList = new ArrayList<Put>();
            while (consumerRecordIterator.hasNext()) {
                ConsumerRecord cr = consumerRecordIterator.next();
                String key = cr.key().toString();
                String row = cr.value().toString();
                LOG.info("start Write data to ES and HBase.....");
                String[] elements = row.split(",");
                Put put = new Put(Bytes.toBytes(elements[1]));
                //将数据添加至hbase库
                put.addColumn(Bytes.toBytes("Basic"), Bytes.toBytes("name"), Bytes.toBytes(elements[0]));
                put.addColumn(Bytes.toBytes("Basic"), Bytes.toBytes("id"), Bytes.toBytes(elements[1]));
                put.addColumn(Bytes.toBytes("Basic"), Bytes.toBytes("age"), Bytes.toBytes(elements[2]));
                put.addColumn(Bytes.toBytes("Basic"), Bytes.toBytes("sex"), Bytes.toBytes(elements[3]));
                if(key.equals("hotel")){
                    //添加索引(姓名,地址,时间)
                    ESSearch.putData(restClient,elements[1], elements[0], elements[4], elements[5]);
                    //将数据添加至hbase库
                    put.addColumn(Bytes.toBytes("OtherInfo"), Bytes.toBytes("hotelAddr"), Bytes.toBytes(elements[4]));
                    put.addColumn(Bytes.toBytes("OtherInfo"), Bytes.toBytes("checkInTime"), Bytes.toBytes(elements[5]));
                    put.addColumn(Bytes.toBytes("OtherInfo"), Bytes.toBytes("checkOutTime"), Bytes.toBytes(elements[6]));
                    put.addColumn(Bytes.toBytes("OtherInfo"), Bytes.toBytes("acquaintance"), Bytes.toBytes(elements[7]));
                }else if(key.equals("internet")){
                    //添加索引(姓名,地址,时间)
                    ESSearch.putData(restClient,elements[1], elements[0], elements[4], elements[5]);
                    //将数据添加至hbase库
                    put.addColumn(Bytes.toBytes("OtherInfo"), Bytes.toBytes("barAddr"), Bytes.toBytes(elements[4]));
                    put.addColumn(Bytes.toBytes("OtherInfo"), Bytes.toBytes("internetDate"), Bytes.toBytes(elements[5]));
                    put.addColumn(Bytes.toBytes("OtherInfo"), Bytes.toBytes("timeSpent"), Bytes.toBytes(elements[6]));
                }else{
                    //添加索引(姓名,地址,时间)
                    ESSearch.putData(restClient,elements[1], elements[0], elements[4], elements[5]);
                    //将数据添加至hbase库
                    put.addColumn(Bytes.toBytes("OtherInfo"), Bytes.toBytes("bayonetAddr"), Bytes.toBytes(elements[4]));
                    put.addColumn(Bytes.toBytes("OtherInfo"), Bytes.toBytes("checkDate"), Bytes.toBytes(elements[5]));
                    put.addColumn(Bytes.toBytes("OtherInfo"), Bytes.toBytes("tripType"), Bytes.toBytes(elements[6]));
                }
                putList.add(put);
            }
            if (putList.size() > 0) {
                LOG.info("Write data to HBase.....");
                table.put(putList);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
