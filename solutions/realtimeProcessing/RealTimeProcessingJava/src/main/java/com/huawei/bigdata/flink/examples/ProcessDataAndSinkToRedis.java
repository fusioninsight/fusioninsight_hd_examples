package com.huawei.bigdata.flink.examples;

import com.huawei.bigdata.flink.examples.utils.Const;
import com.huawei.bigdata.flink.examples.utils.LoginUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.*;

public class ProcessDataAndSinkToRedis
{
    public static void main(String[] args) throws Exception
    {
        System.out.println("use command as: ");
        System.out.println("./bin/flink run --class com.huawei.bigdata.flink.examples.ProcessDataAndSinkToRedis /opt/test.jar  --topic testTopic --bootstrap.servers 189.211.69.32:21005");
        System.out.println("./bin/flink run --class com.huawei.bigdata.flink.examples.ProcessDataAndSinkToRedis /opt/test.jar  --topic testTopic --bootstrap.servers 189.211.69.32:21007 --security.protocol SASL_PLAINTEXT --sasl.kerberos.service.name kafka");
        System.out.println("./bin/flink run --class com.huawei.bigdata.flink.examples.ProcessDataAndSinkToRedis /opt/test.jar  --topic testTopic --bootstrap.servers 189.211.69.32:21008 --security.protocol SSL --ssl.truststore.location /home/truststore.jks --ssl.truststore.password huawei");
        System.out.println("./bin/flink run --class com.huawei.bigdata.flink.examples.ProcessDataAndSinkToRedis /opt/test.jar  --topic testTopic --bootstrap.servers 189.211.69.32:21009 --security.protocol SASL_SSL --sasl.kerberos.service.name kafka --ssl.truststore.location /home/truststore.jks --ssl.truststore.password huawei");
        System.out.println("******************************************************************************************");
        System.out.println("<windowTime> is the width of the window, time as minutes");
        System.out.println("<topic> is the kafka topic name");
        System.out.println("<bootstrap.servers> is the ip:port list of brokers");
        System.out.println("******************************************************************************************");


        //StreamExecutionEnvironment 执行流程序的上下文。环境提供了控制作业执行的方法（例如设置并行性或容错/检查点参数）以及与外部世界交互（数据访问）。
        // 构造执行环境,操作的并行度1
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        ParameterTool paraTool = ParameterTool.fromArgs(args);
        //windowTime设置窗口时间值得大小1，（时间单位在后面处理的地方，可设置：天、时、分、秒、毫秒）
        final Integer windowTime = paraTool.getInt("windowTime", 1);


        DataStream<String> messageStream = env.addSource(new FlinkKafkaConsumer010<>(paraTool.get("topic"), new SimpleStringSchema(), paraTool.getProperties()));
        messageStream.map(new MapFunction<String, TransactionRecord>()
        {
            @Override
            public TransactionRecord map(String value) throws Exception
            {
                return getRecord(value);
            }
        }).assignTimestampsAndWatermarks(new Record2TimestampExtractor()).filter(new FilterFunction<TransactionRecord>()
        {
            @Override
            public boolean filter(TransactionRecord value) throws Exception
            {
                return value.payResult.equals("success");
            }
        }).keyBy(new TransactionRecordSelector()).window(TumblingEventTimeWindows.of(Time.seconds(windowTime))).reduce(new ReduceFunction<TransactionRecord>()
        {
            @Override
            public TransactionRecord reduce(TransactionRecord value1, TransactionRecord value2) throws Exception
            {
                value1.price += value2.price;
                return value1;
            }
        }).addSink(new RedisSink());

        env.execute();
    }
    //将flink计算的结果插入到redis
    private static class RedisSink extends
            RichSinkFunction<TransactionRecord> {
        private transient JedisCluster client;
        private String redisKey = "RedisSinkTest1";

        //open方法是初始化方法,会在invoke方法之前执行,执行一次
        public void open(Configuration parameters)throws Exception
        {
            //安全认证 redis为非安全模式可删除该部分
            try {
                System.setProperty("redis.authentication.jaas", "true");
                if (System.getProperty("redis.authentication.jaas", "false").equals("true")) {
                    //需要把principal修改成自己的用户名
                    String principal = "fan651";
                    //需要从FI Manager 下载对应用户的认证凭据 解压后放入环境中Flink 客户端conf/目录下 启动yarn seesion时 -t conf/携带到yarn上
                    String keytabPath = "conf/user.keytab";
                    String krb5Path = "conf/krb5.conf";
                    LoginUtil.setJaasFile(principal, keytabPath);
                    LoginUtil.setKrb5Config(krb5Path);
                }
            } catch (IOException e) {
                System.out.println("Failed to init security configuration"+e);
                return;
            }

            super.open(parameters);
            if(client != null){
                System.out.println("Redis already connected......");
                return;
            }
            //通过指定集群中一个或多个实例的IP跟端口号，创建JedisCluster实例。
            // 注意把Const接口中的配置修改为自己环境的IP和对应端口
            Set<HostAndPort> hosts = new HashSet<HostAndPort>();
            hosts.add(new HostAndPort(Const.IP_1, Const.PORT_1));
            hosts.add(new HostAndPort(Const.IP_2, Const.PORT_2));
            // add more host...

            // 连接、请求超时时长，时间单位ms
            int timeout = 5000;
            //JedisCluster封装了java访问redis集群的各种操作，包括初始化连接、请求重定向等。
            client = new JedisCluster(hosts, timeout);

            System.out.println("JedisCluster init success");
        }

        //实时更新数据，并获取打印显示
        public  void invoke(TransactionRecord transactionRecord, SinkFunction.Context context) throws Exception {
            try {
                //给某商品transactionRecord.type的值加上一个增量transactionRecord.price
                client.zincrby(redisKey,transactionRecord.price, transactionRecord.type);
                //获取排行数据
                Set<String> setValues = client.zrevrange(redisKey, 0, -1);
                List<String> result = new ArrayList(setValues);
                StringBuffer sb = new StringBuffer();
                for (int i = 0; i < result.size(); i++) {
                    sb.append("Top" +(i+1)+ " : "+ result.get(i) + "-" + client.zscore(redisKey, result.get(i)) + "  ");
                }
                System.out.println(sb.toString());
            }catch (Exception e){
                e.printStackTrace();
            }
        }

        //close()是tear down的方法,在销毁时执行,关闭连接
        public void close()throws Exception
        {
            super.close();
            if(client != null){
                System.out.println("Jediscluster close!!!");
                client.close();
            }
        }
    }


    private static class TransactionRecordSelector implements KeySelector<TransactionRecord, Tuple2<String, String>>
    {
        public Tuple2<String, String> getKey(TransactionRecord value) throws Exception
        {
            return Tuple2.of(value.type, value.payResult);
        }
    }

    private static TransactionRecord getRecord(String line)
    {
        String[] elems = line.split(",");
        assert elems.length == 4;
        return new TransactionRecord(elems[0], elems[1], Integer.parseInt(elems[2]), elems[3]);
    }

    public static class TransactionRecord
    {
        private String type;
        private String payResult;
        private Integer price;
        private String time;

        public TransactionRecord(String type, String payResult, Integer price, String time)
        {
            this.type = type;
            this.payResult = payResult;
            this.price = price;
            this.time = time;
        }

        public String toString()
        {
            return "type: " + type + "  payResult: " + payResult + "  price: " + price.toString() + "  time: "+ time;
        }
    }

    // class to set watermark and timestamp
    private static class Record2TimestampExtractor implements AssignerWithPunctuatedWatermarks<TransactionRecord>
    {

        // add tag in the data of datastream elements
        public long extractTimestamp(TransactionRecord element, long previousTimestamp)
        {
            return System.currentTimeMillis();
        }

        // give the watermark to trigger the window to execute, and use the value to check if the window elements is ready
        public Watermark checkAndGetNextWatermark(TransactionRecord element, long extractedTimestamp)
        {
            return new Watermark(extractedTimestamp - 1);
        }
    }

    private static String getResource(String name) {
        ClassLoader cl = ProcessDataAndSinkToRedis.class.getClassLoader();
        if (cl == null) {
            return null;
        }
        URL url = cl.getResource(name);
        if (url == null) {
            return null;
        }

        try {
            return URLDecoder.decode(url.getPath(), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            return null;
        }
    }

}
