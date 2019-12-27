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
import java.util.*;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;

import static com.huawei.bigdata.security.LoginUtil.isSecurityModel;
import static com.huawei.bigdata.security.LoginUtil.securityPrepare;

/**
 * 消费者类
 */
public class ConsumerMultThread extends Thread {
    private static Logger LOG = Logger.getLogger(ConsumerMultThread.class);

    private static final String USER_KEYTAB_FILE = "用户自己申请的机机账号keytab文件名称";

    private static final String USER_PRINCIPAL = "用户自己申请的机机账号名称";

    // 并发的线程数
    private static int CONCURRENCY_THREAD_NUM = 2;

    // 待消费的topic
    private String topic;

    // 一次请求的最大等待时间
    private final int waitTime = 1000;

    // Broker连接地址
    private final String bootstrapServers = "bootstrap.servers";

    // Group id
    private final String groupId = "group.id";

    // 消息内容使用的反序列化类
    private final String valueDeserializer = "value.deserializer";

    // 消息Key值使用的反序列化类
    private final String keyDeserializer = "key.deserializer";

    // 协议类型:当前支持配置为SASL_PLAINTEXT或者PLAINTEXT
    private final String securityProtocol = "security.protocol";

    // 服务名
    private final String saslKerberosServiceName = "sasl.kerberos.service.name";

    // 是否自动提交offset
    private final String enableAutoCommit = "enable.auto.commit";

    // 自动提交offset的时间间隔
    private final String autoCommitIntervalMs = "auto.commit.interval.ms";

    // 会话超时时间
    private final String sessionTimeoutMs = "session.timeout.ms";

    private static final Properties props = new Properties();

    public ConsumerMultThread(String topic) {
        this.topic = topic;
    }

    @Override
    public void run() {
        for (int threadNum = 0; threadNum < CONCURRENCY_THREAD_NUM; threadNum++) {
            ConsumerThread consumerThread = new ConsumerThread(topic, threadNum);
            consumerThread.start();
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

        // 启动消费线程，其中KafkaProperties.topic为待消费的topic名称
        ConsumerMultThread consumerThread = new ConsumerMultThread(KafkaProperties.topic);
        consumerThread.start();
    }

    /**
     * 消费者线程类
     */
    private class ConsumerThread extends Thread {
        private int consumerThreadId;

        private String topic;

        private final KafkaConsumer<Integer, String> consumer;


        public ConsumerThread(String topic, int threadNum) {
            this.topic = topic;
            this.consumerThreadId = threadNum;
            KafkaProperties kafkaProc = KafkaProperties.getInstance();
            // Broker连接地址
            props.put(bootstrapServers, kafkaProc.getValues(bootstrapServers, "localhost:21008"));
            // Group id
            props.put(groupId, "DemoConsumer");
            // 是否自动提交offset
            props.put(enableAutoCommit, "true");
            // 自动提交offset的时间间隔
            props.put(autoCommitIntervalMs, "1000");
            // 会话超时时间
            props.put(sessionTimeoutMs, "30000");
            // 消息Key值使用的反序列化类
            props.put(keyDeserializer, "org.apache.kafka.common.serialization.IntegerDeserializer");
            // 消息内容使用的反序列化类
            props.put(valueDeserializer, "org.apache.kafka.common.serialization.StringDeserializer");
            // 安全协议类型
            props.put(securityProtocol, kafkaProc.getValues(securityProtocol, "SASL_PLAINTEXT"));
            // 服务名
            props.put(saslKerberosServiceName, "kafka");
            consumer = new KafkaConsumer<Integer, String>(props);
        }

        @Override
        public void run() {
            LOG.info("Consumer start.");
            // 订阅
            try {
                consumer.subscribe(Collections.singletonList(this.topic));

                // 消息消费请求
                while (true) {
                    ConsumerRecords<Integer, String> records = consumer.poll(waitTime);
                    // 消息处理
                    for (ConsumerRecord<Integer, String> record : records) {
                        LOG.info("[NewConsumerExample], Received message: (" + record.key() + ", " + record.value() + ") at offset "
                            + record.offset());
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    consumer.close();
                    LOG.info("Consumer end.");
                } catch (Exception e) {
                    LOG.info("Error in closing consumer.", e);
                }
            }
        }
    }
}
