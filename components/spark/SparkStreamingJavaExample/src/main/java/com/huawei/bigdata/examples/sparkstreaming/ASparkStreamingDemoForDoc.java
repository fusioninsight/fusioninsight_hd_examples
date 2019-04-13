package com.huawei.bigdata.examples.sparkstreaming;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import com.huawei.bigdata.security.kerberos.LoginUtil;

import scala.Tuple2;

import java.util.Arrays;

public class ASparkStreamingDemoForDoc {
    public static void main(String[] args) throws Exception
    {
        //******认证 Start*******
        //本地调试使用。实际运行任务，推荐使用spark-submit方式提交。
        Configuration hadoopConf = new Configuration();
        String krb5Conf =  ASparkStreamingDemoForDoc.class.getClassLoader().getResource("krb5.conf").getPath();
        String keyTab = ASparkStreamingDemoForDoc.class.getClassLoader().getResource("user.keytab").getPath();
        LoginUtil.login("zlt", keyTab, krb5Conf, hadoopConf);
        //******认证 End********

        //配置checkpoint，保存任务元数据和计算结果，用于任务重启后快速恢复，继续之前的计算。
        String checkPoint = "/tmp/sparkstreaming/testuser/checkpoint/";//默认是指HDFS上的路径
        Integer batchTime = 5;
        //配置流处理批次的时间间隔
        Duration batchDuration = Durations.seconds(batchTime);

        //配置任务名称为ASparkStreamingDemoForDoc；
        // 运行方式为本地（local），方便调试；
        // [2]指定启动的线程数量，线程数要两个以上，一个用于接收数据，一个用于数据处理。
        SparkConf sparkConf = new SparkConf().setAppName("ASparkStreamingDemoForDoc").setMaster("local[2]");
        //创建sparkstreaming运行上下文，作为数据处理的主入口
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkConf, batchDuration);
        //配置checkpoint路径
        javaStreamingContext.checkpoint(checkPoint);

        //在187.7.67.6主机上执行nc -lk 9999，启动console的输入，通过键盘输入，作为输入流。
        //创建以socket数据流作为输入来源的DStream
        JavaDStream<String> lines = javaStreamingContext.socketTextStream("187.7.67.6", 9999);

        //通过flatMap将输入的文本流以空格作为分隔符转换为List对象
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String> (){
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });

        //通过mapToPair将单词数组，映射为key, value对：<单词，1>，目的是对每个单词打点，为后面的统计做准备
        JavaPairDStream<String, Integer> keyValue = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        //通过reduceByKey将相同的单词（key）进行合并，计算每个单词出现的总数
        JavaPairDStream<String, Integer> counts =  keyValue.reduceByKey(new Function2<Integer, Integer, Integer> () {
            public Integer call(Integer int1, Integer int2) throws Exception {
                return int1 + int2;
            }
        });

        //在console窗口打印统计结果
        counts.print();

        //前面为业务流程规划

        //启动任务，开始执行业务处理
        javaStreamingContext.start();
        //保证任务持续运行，直到被手动关闭。期间每隔5秒做一次统计。
        javaStreamingContext.awaitTermination();
        //关闭任务，清理资源
        javaStreamingContext.close();
    }
}
