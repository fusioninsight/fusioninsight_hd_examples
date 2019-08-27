package com.huawei.bigdata.examples.kafka.demofordoc;

import com.huawei.bigdata.security.LoginUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.FileInputStream;
import java.util.Properties;

import static java.lang.Thread.sleep;

//执行producer之前，先参考shell/createTopic.sh创建topic，并配置权限。多个producer可以向一个topic写入。

/*
 * 用于配合文档讲解最基本的发送消息接口KafkaProducer如何使用。
 * 样例需要先启动consumer，再启动producer
 */

public class  AProducerDemoForDoc {
    static {
        PropertyConfigurator.configure(AProducerDemoForDoc.class.getClassLoader().getResource("conf/log4j.properties").getPath());
    }
    private static final Logger LOG = LoggerFactory.getLogger(AProducerDemoForDoc.class);
    private static Properties producerProps = new Properties();

    public static void main(String args[])
    {
        //******认证 Start*******
        //安全模式需要，普通模式可以删除
        try {
            String krb5Conf =  AConsumerDemoForDoc.class.getClassLoader().getResource("conf/krb5.conf").getPath();
            String keyTab = AConsumerDemoForDoc.class.getClassLoader().getResource("conf/user.keytab").getPath();

            //使用jaas文件进行认证，生成jaas.conf配置文件，并配置到JVM系统参数中
            LoginUtil.setJaasFile("lyysxg", keyTab);

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

        //Producer业务代码
        //加载从集群下载的客户端配置文件，与服务端对接
        try
        {
            producerProps.load(new FileInputStream(AProducerDemoForDoc.class.getClassLoader().getResource("conf/producer.properties").getPath()));
        }
        catch (Exception e)
        {
            LOG.error(e.getCause().toString());
        }

        //配置当前Producer的定制化配置参数
        producerProps.put("acks", "all"); //服务端多少副本保存成功后，返回响应。0：不需要server相应，1：一个副本，all：所有副本
        producerProps.put("retries", 1); //消息发送失败后，重试的次数
        producerProps.put("batch.size", 16384); //向同一个partition批量发送消息的阈值，达到这个值后就会发送
        producerProps.put("linger.ms", 10);//每次发送消息的时间间隔
        producerProps.put("buffer.memory", 33554432);//保存还未来得及发送到服务端的消息的缓存大小。
        //发送时间间隔、partition数量、每个partition批量发送的阈值、缓存大小四个参数需要结合硬件配置、系统响应时延、带宽占用统筹考虑。
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer"); //根据数据类型确定序列化方式
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); //根据数据类型确定序列化方式
        //指定分区处理类，用于选择消息发送到那个分区，可以根据业务需要定制。没有特殊需要可以使用默认的kafka.producer.DefaultPartitioner
        //partitioner.class=kafka.producer.DefaultPartitioner


        //根据配置文件创建producer，创建的过程就会进行kerberos认证。认证过程由Kafka API内部实现。
        KafkaProducer producer = new KafkaProducer<Integer, String>(producerProps);

        //向test-topic生产消息，消息的key是messageNo的值，value是messageNo的字符串
        for (int messageNo = 0; messageNo < 100; messageNo++)
        {
            producer.send(new ProducerRecord<Integer, String>("test-topic", messageNo, Integer.toString(messageNo)));
            LOG.info("The Producer have send {} messages", messageNo);

            //send的消息会缓存起来，达到发送时间间隔或最大缓存才会真正发送到服务端
            //样例代码执行时间很短，进程关闭了，还没达到发送条件，会导致消息没有实际发送
            //通过sleep保证进程生命周期内，消息被真正发送。
            try {sleep(10);} catch (Exception e) {LOG.error(e.toString());}
        }

    }
}
