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

import java.util.Properties;
import org.apache.log4j.Logger;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class ProducerMultThread extends Thread
{
    private static Logger LOG = Logger.getLogger(ProducerMultThread.class);

    // 并发的线程数
    private static final int PRODUCER_THREAD_COUNT = 2;

    private final String topic;

    private static final Properties props = new Properties();

    public ProducerMultThread(String produceToTopic)
    {

        topic = produceToTopic;
    }

    /**
     * 启动多个线程进行发送
     */
    public void run()
    {
        // 指定的线程号，仅用于区分不同的线程
        for (int threadNum = 0; threadNum < PRODUCER_THREAD_COUNT; threadNum++)
        {
            ProducerThread producerThread = new ProducerThread(topic, threadNum);
            producerThread.start();

        }

    }

    public static void main(String[] args)
    {
        ProducerMultThread producerMultThread = new ProducerMultThread(KafkaProperties.topic);
        producerMultThread.start();
    }

    /**
     * 生产者线程类
     */
    private class ProducerThread extends Thread
    {
        private int sendThreadId = 0;

        private String sendTopic = null;

        private final kafka.javaapi.producer.Producer<String, String> producer;

        /**
         * 生产者线程类构造方法
         * @param topicName Topic名称
         * @param threadNum 线程号
         */
        public ProducerThread(String topicName, int threadNum)
        {
            this.sendThreadId = threadNum;
            this.sendTopic = topicName;

            // 指定Partition发送
            props.put("partitioner.class", "com.huawei.bigdata.kafka.example.SimplePartitioner");

            // 序列化类
            props.put("serializer.class", "kafka.serializer.StringEncoder");

            // 发送模式配置为同步
            props.put("producer.type", "sync");

            // Broker列表
            props.put("metadata.broker.list",
                    KafkaProperties.getInstance().getValues("metadata.broker.list", "localhost:9092"));

            // 指定ACK响应返回（0:不等待确认；1:等待leader节点写入本地成功；-1:等待所有同步列表中的follower节点确认结果）
            props.put("request.required.acks", "1");

            // 创建生产者对象
            producer = new kafka.javaapi.producer.Producer<String, String>(new ProducerConfig(props));
        }

        public void run()
        {
            LOG.info("Producer: start.");

            // 用于记录消息条数
            int messageCount = 1;

            // 每个线程发送的消息条数
            int messagesPerThread = 5;
            while (messageCount <= messagesPerThread)
            {

                // 待发送的消息内容
                String messageStr = new String("Message_" + sendThreadId + "_" + messageCount);

                // 此处对于同一线程指定相同Key值，确保每个线程只向同一个Partition生产消息
                String key = String.valueOf(sendThreadId);

                // 消息发送
                producer.send(new KeyedMessage<String, String>(sendTopic, key, messageStr));
                LOG.info("Producer: send " + messageStr + " to " + sendTopic + " with key: " + key);
                messageCount++;

                // 每隔1s，发送1条消息
                try
                {
                    Thread.sleep(1000);
                }
                catch (InterruptedException e)
                {
                    e.printStackTrace();
                }
            }

            try
            {
                producer.close();
                LOG.info("Producer " + this.sendThreadId + " closed.");
            }
            catch (Throwable e)
            {
                LOG.error("Error when closing producer", e);
            }

        }
    }

}