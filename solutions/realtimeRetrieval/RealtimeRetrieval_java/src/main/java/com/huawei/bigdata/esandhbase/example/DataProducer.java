package com.huawei.bigdata.esandhbase.example;

import com.huawei.bigdata.security.LoginUtilForKafka;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.util.Properties;

/*
 * 执行producer之前，先参考shell/createTopic.sh创建topic，并配置权限。多个producer可以向一个topic写入。
 * 样例需要先启动KafkaStreaming，再启动DataProducer
 */
public class DataProducer {
    static {
        PropertyConfigurator.configure(DataProducer.class.getClassLoader().getResource("log4j.properties").getPath());
    }
    private static final Logger LOG = LoggerFactory.getLogger(DataProducer.class);
    private static Properties producerProps = new Properties();

    public static void main(String args[]) throws Exception {
        //加载从集群下载的客户端配置文件，与服务端对接
        producerProps.load(new FileInputStream(DataProducer.class.getClassLoader().getResource("producer.properties").getPath()));

        //安全模式需要，普通模式可以删除
        String krb5Conf =  DataProducer.class.getClassLoader().getResource("krb5.conf").getPath();
        String keyTab = DataProducer.class.getClassLoader().getResource("user.keytab").getPath();
        LoginUtilForKafka.setJaasFile(producerProps.getProperty("userName"), keyTab);
        LoginUtilForKafka.setKrb5Config(krb5Conf);
        LoginUtilForKafka.setZookeeperServerPrincipal("zookeeper/hadoop.hadoop.com");

        //加载从集群下载的客户端配置文件，与服务端对接
       // producerProps.load(new FileInputStream(DataProducer.class.getClassLoader().getResource("producer.properties").getPath()));
        //Producer业务代码
        //配置当前Producer的定制化配置参数
        producerProps.put("acks", "all"); //服务端多少副本保存成功后，返回响应。0：不需要server相应，1：一个副本，all：所有副本
        producerProps.put("retries", 1); //生产者发送失败后，重试的次数
        producerProps.put("batch.size", 16384); //向同一个partition批量发送消息的阈值，达到这个值后就会发送
        producerProps.put("linger.ms", 100);//每次发送消息的时间间隔
        producerProps.put("buffer.memory", 33554432);//保存还未来得及发送到服务端的消息的缓存大小。
        //发送时间间隔、partition数量、每个partition批量发送的阈值、缓存大小四个参数需要结合硬件配置、系统响应时延、带宽占用统筹考虑。
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer"); //根据数据类型确定序列化方式
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer"); //根据数据类型确定序列化方式
        // 协议类型:当前支持配置为SASL_PLAINTEXT或者PLAINTEXT
        producerProps.put("security.protocol",  "SASL_PLAINTEXT");
        // 服务名
        producerProps.put("sasl.kerberos.service.name", "kafka");

        //根据配置文件创建producer，创建的过程就会进行kerberos认证。认证过程由Kafka API内部实现。
        KafkaProducer producer = new KafkaProducer<Integer, String>(producerProps);
        //向testTopic生产消息
        String filePath = producerProps.getProperty("inputPath");
        String topic = producerProps.getProperty("topic");
        for (int m = 0; m < Integer.MAX_VALUE / 2; m++) {
            File dir = new File(filePath);
            File[] files = dir.listFiles();
            if (files != null){
                for(File file : files){
                    if(file.isDirectory()){
                        System.out.println(file.getName() + "This is a directory!");
                    }else{
                        if (file.getName().contains("hotel")){
                            BufferedReader reader = null;
                            reader = new BufferedReader(new FileReader(filePath+file.getName()));
                            String tempString = null;
                            while ((tempString = reader.readLine()) != null) {
                                //Blank line judgment
                                if (!tempString.isEmpty()) {
                                    ProducerRecord producerRecord = new ProducerRecord<String, String>(topic, "hotel", tempString);
                                    LOG.info("hotel_info:" +tempString);
                                    System.out.println("hotel_info:" +tempString);
                                    try
                                    {
                                        Thread.sleep(500);
                                    }
                                    catch (InterruptedException e)
                                    {
                                        e.printStackTrace();
                                    }
                                    producer.send(producerRecord);
                                }
                            }

                            reader.close();
                        }
                        else if(file.getName().contains("internet")){
                            BufferedReader reader = null;
                            reader = new BufferedReader(new FileReader(filePath+file.getName()));
                            String tempString = null;
                            while ((tempString = reader.readLine()) != null) {
                                //Blank line judgment
                                if (!tempString.isEmpty()) {
                                    ProducerRecord producerRecord = new ProducerRecord<String, String>(topic, "internet", tempString);
                                    LOG.info("internet_info:" +tempString);
                                    System.out.println("internet_info:" +tempString);
                                    try
                                    {
                                        Thread.sleep(500);
                                    }
                                    catch (InterruptedException e)
                                    {
                                        e.printStackTrace();
                                    }
                                    producer.send(producerRecord);
                                }
                            }
                            reader.close();

                        }else{
                            BufferedReader reader = null;
                            reader = new BufferedReader(new FileReader(filePath+file.getName()));
                            String tempString = null;
                            while ((tempString = reader.readLine()) != null) {
                                //Blank line judgment
                                if (!tempString.isEmpty()) {
                                    ProducerRecord producerRecord = new ProducerRecord<String, String>(topic, "bayonet", tempString);
                                    LOG.info("bayonet_info:" +tempString);
                                    System.out.println("bayonet_info:" +tempString);
                                    try
                                    {
                                        Thread.sleep(500);
                                    }
                                    catch (InterruptedException e)
                                    {
                                        e.printStackTrace();
                                    }
                                    producer.send(producerRecord);
                                }
                            }
                            reader.close();
                        }
                    }
                }
            }
            try
            {
                Thread.sleep(3000);
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
            }
        }
        producer.close();
    }
}
