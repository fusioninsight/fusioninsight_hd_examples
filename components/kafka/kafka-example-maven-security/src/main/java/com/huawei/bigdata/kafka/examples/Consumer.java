package com.huawei.bigdata.kafka.examples;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.bigdata.security.LoginUtil;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class Consumer extends Thread
{
    private static final Logger LOG = LoggerFactory.getLogger(Consumer.class);

    /**
     * 用户自己申请的机机账号keytab文件名称
     */
    private static final String USER_KEYTAB_FILE = "用户自己申请的机机账号keytab文件名称";

    /**
     * 用户自己申请的机机账号名称
     */
    private static final String USER_PRINCIPAL = "用户自己申请的机机账号名称";

    private ConsumerConnector consumer;

    private String topic;

    public Consumer(String topic)
    {
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig());
        this.topic = topic;
    }

    /**
     * 创建consumer的Config
     * @return [ConsumerConfig]
     */
    private static ConsumerConfig createConsumerConfig()
    {
        KafkaProperties kafkaPros = KafkaProperties.getInstance();
        LOG.info("ConsumerConfig: entry.");

        Properties props = new Properties();

        props.put("zookeeper.connect", kafkaPros.getValues("zookeeper.connect", "localhost:2181"));
        props.put("group.id", kafkaPros.getValues("group.id", "example-group1"));
        props.put("zookeeper.session.timeout.ms", kafkaPros.getValues("zookeeper.session.timeout.ms", "15000"));
        props.put("zookeeper.sync.time.ms", kafkaPros.getValues("zookeeper.sync.time.ms", "2000"));
        props.put("auto.commit.interval.ms", kafkaPros.getValues("auto.commit.interval.ms", "10000"));

        LOG.info("ConsumerConfig: props is " + props);

        return new ConsumerConfig(props);
    }

    public void run()
    {
        LOG.info("Consumer: start.");

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(1));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        LOG.info("Consumerstreams size is : " + streams.size());

        for (KafkaStream<byte[], byte[]> stream : streams)
        {
            ConsumerIterator<byte[], byte[]> it = stream.iterator();

            while (it.hasNext())
            {
                LOG.info("Consumer: receive " + new String(it.next().message()) + " from " + topic);
            }
        }

        LOG.info("Consumer End.");
    }

    public void shutdown()
    {
        if (consumer != null)
        {
            LOG.info("Consumer: shutdown.");

            consumer.shutdown();
        }
    }

    public static Boolean isSecurityModel()
    {
        Boolean isSecurity = false;
        String krbFilePath = System.getProperty("user.dir") + File.separator + "conf" + File.separator
                + "kafkaSecurityMode";

        Properties securityProps = new Properties();

        // file does not exist.
        if (!isFileExists(krbFilePath))
        {
            return isSecurity;
        }

        try
        {
            securityProps.load(new FileInputStream(krbFilePath));
            if ("yes".equalsIgnoreCase(securityProps.getProperty("kafka.client.security.mode")))
            {
                isSecurity = true;
            }
        }
        catch (Exception e)
        {
            LOG.info("The Exception occured : {}.", e);
        }

        return isSecurity;
    }

    /*
     * 判断文件是否存在
     */
    private static boolean isFileExists(String fileName)
    {
        File file = new File(fileName);

        return file.exists();
    }

    public static void main(String[] args)
    {
        if (isSecurityModel())
        {
            try
            {
                LOG.info("Securitymode start.");

                // !!注意，安全认证时，需要用户手动修改为自己申请的机机账号
                securityPrepare();
            }
            catch (IOException e)
            {
                LOG.error("Security prepare failure.");
                LOG.error("The Exception occured.", e);
                return;
            }
            LOG.info("Security prepare success.");
        }

        Consumer consumerThread = new Consumer(KafkaProperties.topic);
        consumerThread.start();

        // 等到5s后将consumer关闭，实际执行过程中可修改
        try
        {
            Thread.sleep(5000);
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
        finally
        {
            consumerThread.shutdown();
        }
    }

    public static void securityPrepare() throws IOException
    {
        String filePath = System.getProperty("user.dir") + File.separator + "conf" + File.separator;
        String krbFile = filePath + "krb5.conf";
        String userKeyTableFile = filePath + USER_KEYTAB_FILE;

        // windows路径下分隔符替换
        userKeyTableFile = userKeyTableFile.replace("\\", "\\\\");
        krbFile = krbFile.replace("\\", "\\\\");

        LoginUtil.setKrb5Config(krbFile);
        LoginUtil.setZookeeperServerPrincipal("zookeeper/hadoop.hadoop.com");
        LoginUtil.setJaasFile(USER_PRINCIPAL, userKeyTableFile);
    }
}
