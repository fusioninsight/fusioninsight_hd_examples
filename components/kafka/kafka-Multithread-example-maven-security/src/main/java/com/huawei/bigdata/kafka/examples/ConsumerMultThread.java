/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.huawei.bigdata.kafka.examples;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.huawei.bigdata.security.LoginUtil;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

/**
 * 消费者类
 */
public class ConsumerMultThread extends Thread
{
    private static Logger LOG = Logger.getLogger(ConsumerMultThread.class);

    /**
     * 用户自己申请的机机账号keytab文件名称
     */
    private static final String USER_KEYTAB_FILE = "用户自己申请的机机账号keytab文件名称";

    /**
     * 用户自己申请的机机账号名称
     */
    private static final String USER_PRINCIPAL = "用户自己申请的机机账号名称";

    // 并发的线程数
    private static int CONCURRENCY_THREAD_NUM = 2;

    private ConsumerConnector consumer;

    // 待消费的topic
    private String topic;

    public ConsumerMultThread(String topic)
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

        // ZooKeeper的地址，从server.properties中获取
        props.put("zookeeper.connect", kafkaPros.getValues("zookeeper.connect", "localhost:2181"));

        // 本Consumer的组id，从consumer.properties中获取
        props.put("group.id", kafkaPros.getValues("group.id", "example-group1"));

        // ZooKeeper的session超时时间，从server.properties中获取
        props.put("zookeeper.session.timeout.ms", kafkaPros.getValues("zookeeper.session.timeout.ms", "15000"));

        // zookeeper.sync.time.ms，从server.properties中获取
        props.put("zookeeper.sync.time.ms", kafkaPros.getValues("zookeeper.sync.time.ms", "2000"));

        // Consumer客户端提交offset的默认时间，从consumer.properties中获取
        props.put("auto.commit.interval.ms", kafkaPros.getValues("auto.commit.interval.ms", "10000"));

        // 当zookeeper中没有本Consumer组的offset或者offset超出合法范围后，应从何处开始消费数据。
        // "smallest"——从头开始消费; "largest"——从当前位置开始消费; 其他值——在Consumer客户端抛异常。
        props.put("auto.offset.reset", "smallest");

        // Consumer客户端存储offset的地方。
        // "zookeeper"——存储在Zookeeper上; "kafka"——存储在Kafka上。
        props.put("offsets.storage", "kafka");

        LOG.info("ConsumerConfig: props is " + props);

        return new ConsumerConfig(props);
    }

    public void run()
    {
        LOG.info("Consumer: start.");

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();

        // 后边的Integer表示启动多少个线程来消费指定的Topic
        // 注意： 当该Integer大于待消费Topic的Partition时，多出的线程将无法消费到数据
        topicCountMap.put(topic, new Integer(CONCURRENCY_THREAD_NUM));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        LOG.info("Consumerstreams size is : " + streams.size());

        // 指定的线程号，仅用于区分不同的线程
        int threadNum = 0;
        for (KafkaStream<byte[], byte[]> stream : streams)
        {
            StreamThread streamThread = new StreamThread(stream, threadNum, topic);
            streamThread.start();
            threadNum++;
        }

        LOG.info("Consumer End.");
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
        // 安全模式下启用
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
                LOG.error(e);
                return;
            }
            LOG.info("Security prepare success.");
        }

        // 启动消费线程，其中KafkaProperties.topic为待消费的topic名称
        ConsumerMultThread consumerThread = new ConsumerMultThread(KafkaProperties.topic);
        consumerThread.start();
    }

    /**
     * 消费者线程类
     *
     */
    private class StreamThread extends Thread
    {
        private KafkaStream<byte[], byte[]> stream = null;

        private int threadNum = 0;

        private String topic = null;

        /**
         * 消费者线程类构造方法
         * @param stream 流
         * @param threadNum 线程号
         * @param topic topic
         */
        public StreamThread(KafkaStream<byte[], byte[]> stream, int threadNum, String topic)
        {
            this.stream = stream;
            this.threadNum = threadNum;
            this.topic = topic;
        }

        public void run()
        {
            LOG.info("Stream Thread " + this.threadNum + " Start.");

            ConsumerIterator<byte[], byte[]> it = stream.iterator();

            LOG.info("Stream Thread " + this.threadNum + " receiveing...");

            // 阻塞方法，即当该线程消费的Partition上无最新数据的情况下，则一直等待，直到手动停止
            while (it.hasNext())
            {
                LOG.info("Stream Thread " + this.threadNum + " receive " + new String(it.next().message()) + " from "
                        + topic);
            }
        }
    }
}
