package com.huawei.bigdata.examples.kafka.demofordoc;

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

//执行consumer之前，先参考shell/createTopic.sh配置权限

/*
 * 用于配合文档讲解最基本的发送消息接口KafkaConsumer如何使用。
 * 样例需要先启动consumer，再启动producer
 */

public class AConsumerDemoForDoc {

    static {
        PropertyConfigurator.configure(AConsumerDemoForDoc.class.getClassLoader().getResource("conf/log4j.properties").getPath());
    }
    private static final Logger LOG = LoggerFactory.getLogger(AConsumerDemoForDoc.class);
    private static Properties consumerProps = new Properties();

    public static void main(String args[])
    {
        //******认证 Start*******
        //安全模式需要，普通模式可以删除
        try {
            String krb5Conf =  AConsumerDemoForDoc.class.getClassLoader().getResource("conf/krb5.conf").getPath();
            String keyTab = AConsumerDemoForDoc.class.getClassLoader().getResource("conf/user.keytab").getPath();

            //使用jaas文件进行认证，生成jaas.conf配置文件，并配置到JVM系统参数中
            LoginUtil.setJaasFile("TestUser", keyTab);

            //将krb5.conf（链接KDC的配置文件）配置到JVM系统参数中
            LoginUtil.setKrb5Config(krb5Conf);

            //配置ZK服务端principal到JVM系统参数中，访问ZK的时候认证使用
            LoginUtil.setZookeeperServerPrincipal("zookeeper/hadoop.hadoop.com");
            //******认证 End********
        }
        catch (Exception e)
        {
            LOG.error(e.toString());
        }

        //Consumer业务代码
        //加载从集群下载的客户端配置文件，与服务端对接
        try
        {
            consumerProps.load(new FileInputStream(AConsumerDemoForDoc.class.getClassLoader().getResource("conf/consumer.properties").getPath()));
        }
        catch (Exception e)
        {
            LOG.error(e.getCause().toString());
        }

        //配置当前Producer的定制化配置参数
        consumerProps.put("group.id", "test-group"); //属于同一个group的consumer

        //配置是否自动提交offset。为true时，数据被consumer读取后，就会以auto.commit.interval.ms为间隔返回offset。
        //如果要防止consumer端故障，导致数据漏分析，需要设置为false，在数据处理完后在手动提交offset。
        consumerProps.put("enable.auto.commit", "true");
        consumerProps.put("auto.commit.interval.ms", "100"); //offset自动提交间隔
        consumerProps.put("bootstrap.servers", "187.7.66.109:21007,187.7.67.6:21007,187.7.67.88:21007");//使用producer.properties中的值
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer"); //key的序列化处理方式
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"); //value的序列化处理方式

        //根据配置文件创建producer，创建的过程就会进行kerberos认证。认证过程由Kafka API内部实现。
        KafkaConsumer consumer = new KafkaConsumer<Integer, String>(consumerProps);
        consumer.subscribe(Arrays.asList("test-topic"));

        //从test-topic消费消息，消息的key是messageNo的值，value是messageNo的字符串
        while (true)
        {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
                LOG.info("topic={}, partition={}, offset = {}, key = {}, value = {}",
                         record.topic(),record.partition(),record.offset(), record.key(), record.value());
        }
    }
}
