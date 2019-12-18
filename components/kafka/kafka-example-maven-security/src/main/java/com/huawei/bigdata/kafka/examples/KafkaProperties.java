package com.huawei.bigdata.kafka.examples;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;


public class KafkaProperties
{
    private static final Logger LOG = Logger.getLogger(KafkaProperties.class);

    // Topic名称，安全模式下，需要以管理员用户添加当前用户的访问权限
    public final static String topic = "test0001";

    private static Properties serverProps = new Properties();

    private static Properties producerProps = new Properties();

    private static Properties consumerProps = new Properties();

    private static Properties clientProps = new Properties();

    private static KafkaProperties instance = null;

    private KafkaProperties()
    {

        try
        {
            File proFile = new File(this.getClass().getClassLoader().getResource("conf/producer.properties").getPath());

            if (proFile.exists())
            {
                producerProps.load(new FileInputStream(this.getClass().getClassLoader().getResource("conf/producer.properties").getPath()));
            }

            File conFile = new File(this.getClass().getClassLoader().getResource("conf/consumer.properties").getPath());

            if (conFile.exists())
            {
                consumerProps.load(new FileInputStream(this.getClass().getClassLoader().getResource("conf/consumer.properties").getPath()));
            }

            File serFile = new File(this.getClass().getClassLoader().getResource("conf/server.properties").getPath());

            if (serFile.exists())
            {
                serverProps.load(new FileInputStream(this.getClass().getClassLoader().getResource("conf/server.properties").getPath()));
            }

            File cliFile = new File(this.getClass().getClassLoader().getResource("conf/client.properties").getPath());

            if (cliFile.exists())
            {
                clientProps.load(new FileInputStream(this.getClass().getClassLoader().getResource("conf/client.properties").getPath()));
            }
        }
        catch (IOException e)
        {
            LOG.info("The Exception occured.", e);
        }
    }

    public synchronized static KafkaProperties getInstance()
    {
        if (null == instance)
        {
            instance = new KafkaProperties();
        }

        return instance;
    }

    /**
     * 获取参数值
     * @param key properites的key值
     * @param defValue 默认值
     * @return
     */
    public String getValues(String key, String defValue)
    {
        String rtValue = null;

        if (null == key)
        {
            LOG.error("key is null");
        }
        else
        {
            rtValue = getPropertiesValue(key);
        }

        if (null == rtValue)
        {
            LOG.warn("KafkaProperties.getValues return null, key is " + key);
            rtValue = defValue;
        }

        LOG.info("KafkaProperties.getValues: key is " + key + "; Value is " + rtValue);

        return rtValue;
    }

    /**
     * 根据key值获取server.properties的值
     * @param key
     * @return
     */
    private String getPropertiesValue(String key)
    {
        String rtValue = serverProps.getProperty(key);

        // server.properties中没有，则再向producer.properties中获取
        if (null == rtValue)
        {
            rtValue = producerProps.getProperty(key);
        }

        // producer中没有，则再向consumer.properties中获取
        if (null == rtValue)
        {
            rtValue = consumerProps.getProperty(key);
        }

        // consumer没有，则再向client.properties中获取
        if (null == rtValue)
        {
            rtValue = clientProps.getProperty(key);
        }

        return rtValue;
    }
}
