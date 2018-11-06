package com.huawei.bigdata.kafka.examples;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.bigdata.security.LoginUtil;


public class NewProducer extends Thread
{
    private static final Logger LOG = LoggerFactory.getLogger(NewProducer.class);

    private final KafkaProducer<Integer, String> producer;

    private final String topic;

    private final Boolean isAsync;

    private final Properties props = new Properties();

    // Broker地址列表
    private final String bootstrapServers = "bootstrap.servers";

    // 客户端ID
    private final String clientId = "client.id";

    // Key序列化类
    private final String keySerializer = "key.serializer";

    // Value序列化类
    private final String valueSerializer = "value.serializer";

    // 协议类型:当前支持配置为SASL_PLAINTEXT或者PLAINTEXT
    private final String securityProtocol = "security.protocol";

    // 服务名
    private final String saslKerberosServiceName = "sasl.kerberos.service.name";

    // 默认发送20条消息
    private final int messageNumToSend = 100;

    /**
     * 用户自己申请的机机账号名称
     */
    private static final String USER_PRINCIPAL = "kafka001";

    /**
     * 新Producer 构造函数
     * @param topicName Topic名称
     * @param isAsync 是否异步模式发送
     */
    public NewProducer(String topicName, Boolean asyncEnable)
    {

        KafkaProperties kafkaProc = KafkaProperties.getInstance();

        // Broker地址列表
        props.put(bootstrapServers, kafkaProc.getValues(bootstrapServers, "187.5.89.12:21007,187.5.89.47:21007,187.5.89.66:21007,187.5.88.163:21007"));
        // 客户端ID
        props.put(clientId, kafkaProc.getValues(clientId, "DemoProducer"));
        // Key序列化类
        props.put(keySerializer,
                kafkaProc.getValues(keySerializer, "org.apache.kafka.common.serialization.IntegerSerializer"));
        // Value序列化类
        props.put(valueSerializer,
                kafkaProc.getValues(valueSerializer, "org.apache.kafka.common.serialization.StringSerializer"));
        // 协议类型:当前支持配置为SASL_PLAINTEXT或者PLAINTEXT
        props.put(securityProtocol, kafkaProc.getValues(securityProtocol, "SASL_PLAINTEXT"));
        // 服务名
        props.put(saslKerberosServiceName, "kafka");

        producer = new KafkaProducer<Integer, String>(props);
        topic = topicName;
        isAsync = asyncEnable;
    }

    /**
     * 生产者线程执行函数，循环发送消息。
     */
    public void run()
    {
        LOG.info("New Producer: start.");
        int messageNo = 1;
        // 指定发送多少条消息后sleep1秒
        int intervalMessages = 1;

        while (messageNo <= messageNumToSend)
        {
            String messageStr = "Message_" + messageNo;
            long startTime = System.currentTimeMillis();

            // 构造消息记录
            ProducerRecord<Integer, String> record = new ProducerRecord<Integer, String>(topic, messageNo, messageStr);

            if (isAsync)
            {
                try
                {
                    // 异步发送
                    producer.send(record, new DemoCallBack(startTime, messageNo, messageStr));
                }
                catch (Exception e)
                {
                    messageNo--;
                    LOG.error("The Exception occured : {}.", e);
                }
            }
            else
            {
                try
                {
                    // 同步发送
                    producer.send(record).get();
                }
                catch (InterruptedException ie)
                {
                    messageNo--;
                    LOG.error("The InterruptedException occured : {}.", ie);
                }
                catch (ExecutionException ee)
                {
                    messageNo--;
                    LOG.error("The ExecutionException occured : {}.", ee);
                }
                catch (Exception e)
                {
                    messageNo--;
                    LOG.error("The Exception occured : {}.", e);
                }
            }
            messageNo++;

            if (messageNo % intervalMessages == 0)
            {
                // 每发送intervalMessage条消息sleep1秒
                try
                {
                    Thread.sleep(1000);
                }
                catch (InterruptedException e)
                {
                    e.printStackTrace();
                }
                LOG.info("The Producer have send {} messages.", messageNo);
            }
        }

    }

    public static void securityPrepare() throws IOException
    {
        
        String krbFile =  NewProducer.class.getClassLoader().getResource("conf/krb5.conf").getPath();
        String userKeyTableFile = NewProducer.class.getClassLoader().getResource("conf/user.keytab").getPath();

        LoginUtil.setKrb5Config(krbFile);
        LoginUtil.setZookeeperServerPrincipal("zookeeper/hadoop.hadoop.com");
        LoginUtil.setJaasFile(USER_PRINCIPAL, userKeyTableFile);
    }

    public static Boolean isSecurityModel()
    {
        Boolean isSecurity = false;
        String krbFilePath = NewProducer.class.getClassLoader().getResource("conf/kafkaSecurityMode").getPath();

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
        PropertyConfigurator.configure(NewProducer.class.getClassLoader().getResource("conf/log4j.properties").getPath());
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
                LOG.error("The IOException occured.", e);
                return;
            }
            LOG.info("Security prepare success.");
        }

        // 是否使用异步发送模式
        final boolean asyncEnable = false;
        NewProducer producerThread = new NewProducer("test0001", asyncEnable);
        producerThread.start();
    }
}

class DemoCallBack implements Callback
{
    private static Logger LOG = LoggerFactory.getLogger(DemoCallBack.class);

    private long startTime;

    private int key;

    private String message;

    public DemoCallBack(long startTime, int key, String message)
    {
        this.startTime = startTime;
        this.key = key;
        this.message = message;
    }

    /**
     * 回调函数，用于处理异步发送模式下，消息发送到服务端后的处理。
     * @param metadata 元数据信息
     * @param exception 发送异常。如果没有错误发生则为Null。
     */
    @Override
    public void onCompletion(RecordMetadata metadata, java.lang.Exception exception)
    {
        long elapsedTime = System.currentTimeMillis() - startTime;
        if (metadata != null)
        {
            LOG.info("message(" + key + ", " + message + ") sent to partition(" + metadata.partition() + "), "
                    + "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
        }
        else if (exception != null)
        {
            LOG.error("The Exception occured.", exception);
        }

    }
}