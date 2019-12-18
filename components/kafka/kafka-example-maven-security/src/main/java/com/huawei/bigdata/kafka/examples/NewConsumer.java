package com.huawei.bigdata.kafka.examples;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

import kafka.utils.ShutdownableThread;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.apache.log4j.Logger;

import static com.huawei.bigdata.security.LoginUtil.isSecurityModel;
import static com.huawei.bigdata.security.LoginUtil.securityPrepare;

public class NewConsumer extends ShutdownableThread {
    private static final Logger LOG = Logger.getLogger(NewConsumer.class);

    private final KafkaConsumer<Integer, String> consumer;

    private final String topic;

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

    private static final String USER_KEYTAB_FILE = "用户自己申请的机机账号keytab文件名称";

    private static final String USER_PRINCIPAL = "用户自己申请的机机账号名称";

    /**
     * NewConsumer构造函数
     *
     * @param topic 订阅的Topic名称
     */
    public NewConsumer(String topic) {
        super("KafkaConsumerExample", false);
        Properties props = new Properties();

        KafkaProperties kafkaProc = KafkaProperties.getInstance();
        // Broker连接地址
        props.put(bootstrapServers, kafkaProc.getValues(bootstrapServers, "localhost:21007"));
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
        this.topic = topic;
    }

    /**
     * 订阅Topic的消息处理函数
     */
    @Override
    public void doWork() {
        // 订阅
        consumer.subscribe(Collections.singletonList(this.topic));
        // 消息消费请求
        ConsumerRecords<Integer, String> records = consumer.poll(waitTime);
        // 消息处理
        for (ConsumerRecord<Integer, String> record : records) {
            LOG.info("[NewConsumerExample], Received message: (" + record.key() + ", " + record.value() + ") at offset "
                + record.offset());
        }
    }

    public static void main(String[] args) {
        if (isSecurityModel()) {
            try {
                LOG.info("Securitymode start.");

                // !!注意，安全认证时，需要用户手动修改为自己申请的机机账号
                securityPrepare(USER_KEYTAB_FILE, USER_PRINCIPAL);
            } catch (IOException e) {
                LOG.error("Security prepare failure.");
                LOG.error("The IOException occured : {}.", e);
                return;
            }
            LOG.info("Security prepare success.");
        }

        NewConsumer consumerThread = new NewConsumer(KafkaProperties.topic);
        consumerThread.start();

        // 等到60s后将consumer关闭，实际执行过程中可修改
        try {
            Thread.sleep(60000);
        } catch (InterruptedException e) {
            LOG.info("The InterruptedException occured : {}.", e);
        } finally {
            consumerThread.shutdown();
            consumerThread.consumer.close();
        }
    }
}
