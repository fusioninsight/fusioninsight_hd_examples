package com.huawei.bigdata.flink.examples;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class WriteIntoKafka
{
    public static void main(String[] args) throws Exception
    {
        System.out.println("use command as: ");
        System.out.println("./bin/flink run --class com.huawei.bigdata.flink.examples.WriteIntoKafka /opt/test.jar --topic testTopic --bootstrap.servers 189.211.69.32:21005");
        System.out.println("./bin/flink run --class com.huawei.bigdata.flink.examples.WriteIntoKafka /opt/test.jar --topic testTopic --bootstrap.servers 189.211.69.32:21007 --security.protocol SASL_PLAINTEXT --sasl.kerberos.service.name kafka");
        System.out.println("./bin/flink run --class com.huawei.bigdata.flink.examples.WriteIntoKafka /opt/test.jar --topic testTopic --bootstrap.servers 189.211.69.32:21008 --security.protocol SSL --ssl.truststore.location /home/truststore.jks --ssl.truststore.password huawei");
        System.out.println("./bin/flink run --class com.huawei.bigdata.flink.examples.WriteIntoKafka /opt/test.jar --topic testTopic --bootstrap.servers 189.211.69.32:21009 --security.protocol SASL_SSL --sasl.kerberos.service.name kafka --ssl.truststore.location /home/truststore.jks --ssl.truststore.password huawei");
        System.out.println("******************************************************************************************");
        System.out.println("<topic> is the kafka topic name");
        System.out.println("<bootstrap.servers> is the ip:port list of brokers");
        System.out.println("******************************************************************************************");

        //StreamExecutionEnvironment 执行流程序的上下文。环境提供了控制作业执行的方法（例如设置并行性或容错/检查点参数）以及与外部世界交互（数据访问）。
        // 构造执行环境,操作的并行度1
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        ParameterTool paraTool = ParameterTool.fromArgs(args);
        //SimpleStringGenerator生成数据流
        DataStream<String> messageStream = env.addSource(new SimpleStringGenerator());
        //kafka生产者向testTopic生产消息
        messageStream.addSink(new FlinkKafkaProducer010<>(paraTool.get("topic"), new SimpleStringSchema(), paraTool.getProperties()));
        env.execute();
    }
    public static class SimpleStringGenerator implements SourceFunction<String> {
        //商品类型:电子产品, 服装, 食品, 化妆品,医药
        static final String[] TYPE = {"Electronic products", "clothing","food", "cosmetic", "medicine"};
        //支付成功标识
        static final String[] PAY_RESULT = {"success", "failure"};
        static final int COUNT = TYPE.length;
        boolean running = true;
        Random rand = new Random(47);
        @Override
        //rand随机产生订单数据data：商品类型，支付结果标识，价格,下单时间
        public void run(SourceContext<String> ctx) throws Exception {
            while (running) {
                int i = rand.nextInt(COUNT);
                String payResult = PAY_RESULT[rand.nextInt(2)];
                //随机生成价格0-100
                int price = rand.nextInt(100);

                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
                String time = df.format(new Date());// new Date()为获取当前系统时间，也可使用当前时间戳
                String data = TYPE[i] + "," +  payResult + "," + price + "," + time;
                ctx.collect(data);
                System.out.println(data);
                Thread.sleep(1000);
            }
        }
        @Override
        public void cancel() {
            running = false;
        }
    }
}
