/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.huawei.bigdata.kafka.examples;

import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.apache.log4j.Logger;

import static com.huawei.bigdata.security.LoginUtil.isSecurityModel;
import static com.huawei.bigdata.security.LoginUtil.securityPrepare;

public class ProducerMultThread extends Thread {
    private static Logger LOG = Logger.getLogger(ProducerMultThread.class);

    private static final String USER_KEYTAB_FILE = "用户自己申请的机机账号keytab文件名称";

    private static final String USER_PRINCIPAL = "用户自己申请的机机账号名称";

    private static final Properties props = new Properties();

    private final String topic;

    // 并发的线程数
    private static final int PRODUCER_THREAD_COUNT = 2;

    // Key序列化类
    private final String keySerializer = "key.serializer";

    // Value序列化类
    private final String valueSerializer = "value.serializer";

    // Broker集合
    private final String bootstrapServers = "bootstrap.servers";

    // 协议类型:当前支持配置为SASL_PLAINTEXT或者PLAINTEXT
    private final String securityProtocol = "security.protocol";

    // 服务名
    private final String saslKerberosServiceName = "sasl.kerberos.service.name";

    public ProducerMultThread(String topic) {
        this.topic = topic;
    }

    /**
     * 启动多个线程进行发送
     */
    @Override
    public void run() {
        // 指定的线程号，仅用于区分不同的线程
        for (int threadNum = 0; threadNum < PRODUCER_THREAD_COUNT; threadNum++) {
            ProducerThread producerThread = new ProducerThread(this.topic, threadNum);
            producerThread.start();
        }
    }

    public static void main(String[] args) {
        // 安全模式下启用
        if (isSecurityModel()) {
            try {
                LOG.info("Securitymode start.");

                //!!注意，安全认证时，需要用户手动修改为自己申请的机机账号
                securityPrepare(USER_KEYTAB_FILE, USER_PRINCIPAL);
            } catch (IOException e) {
                LOG.error("Security prepare failure.");
                LOG.error(e);
                return;
            }
            LOG.info("Security prepare success.");
        }

        ProducerMultThread producerMultThread = new ProducerMultThread(KafkaProperties.topic);
        producerMultThread.start();
    }

    /**
     * 生产者线程类
     */
    private class ProducerThread extends Thread {
        private int sendThreadId;

        private String sendTopic;

        private final KafkaProducer<Integer, String> producer;

        /**
         * 生产者线程类构造方法
         *
         * @param topicName Topic名称
         * @param threadNum 线程号
         */
        public ProducerThread(String topicName, int threadNum) {
            KafkaProperties kafkaProc = KafkaProperties.getInstance();
            this.sendThreadId = threadNum;
            this.sendTopic = topicName;

            // 指定Partition发送
            props.put("partitioner.class", "com.huawei.bigdata.kafka.examples.SimplePartitioner");
            // 序列化类
            props.put("serializer.class", "kafka.serializer.StringEncoder");
            // Key序列化类
            props.put(keySerializer, "org.apache.kafka.common.serialization.IntegerSerializer");
            // Value序列化类
            props.put(valueSerializer, "org.apache.kafka.common.serialization.StringSerializer");
            // 发送模式配置为同步
            props.put("producer.type", "sync");
            // Broker列表
            props.put(bootstrapServers, kafkaProc.getValues(bootstrapServers, "localhost:21007"));
            // 指定ACK响应返回（0:不等待确认；1:等待leader节点写入本地成功；-1:等待所有同步列表中的follower节点确认结果）
            props.put("request.required.acks", "1");
            // 协议类型:当前支持配置为SASL_PLAINTEXT或者PLAINTEXT
            props.put(securityProtocol, kafkaProc.getValues(securityProtocol, "SASL_PLAINTEXT"));
            // 服务名
            props.put(saslKerberosServiceName, "kafka");

            // 创建生产者对象
            producer = new KafkaProducer<Integer, String>(props);
        }

        @Override
        public void run() {
            LOG.info("Producer: start.");

            // 用于记录消息条数
            int messageCount = 1;

            // 每个线程发送的消息条数
            int messagesPerThread = 500;
            try {
                while (messageCount <= messagesPerThread) {
                    // 待发送的消息内容
                    String messageStr = new String("Message_" + sendThreadId + "_" + messageCount);

                    // 构造消息记录
                    ProducerRecord<Integer, String> record = new ProducerRecord<Integer, String>(sendTopic, sendThreadId, messageStr);

                    // 同步发送
                    producer.send(record);
                    LOG.info("Threadid: " + this.sendThreadId + " has sent " + messageCount + " message.");

                    messageCount++;

                    // 每隔1s，发送1条消息
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    producer.close();
                    LOG.info("Producer closed.");
                } catch (Exception e) {
                    LOG.error("Error in closing producer.", e);
                }
            }
        }
    }
}