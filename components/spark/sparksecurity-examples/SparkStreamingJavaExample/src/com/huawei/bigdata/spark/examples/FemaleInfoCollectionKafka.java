package com.huawei.bigdata.spark.examples;

import java.util.*;

import scala.Tuple2;
import scala.Tuple3;

import com.huawei.spark.streaming.kafka.*;
import kafka.producer.KeyedMessage;
import kafka.serializer.StringDecoder;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.huawei.hadoop.security.LoginUtil;

/**
 * Consumes messages from one or more topics in Kafka.
 * <batchTime> is the Spark Streaming batch duration in seconds.
 * <windowTime> is the width of the window
 * <topics> is a list of one or more kafka topics to consume from
 * <brokers> is for bootstrapping and the producer will only use it for getting metadata
 */

public class FemaleInfoCollectionKafka {
  public static void main(String[] args) throws Exception {

    String userPrincipal = "sparkuser";
    String userKeytabPath = "/opt/FIclient/user.keytab";
    String krb5ConfPath = "/opt/FIclient/KrbClient/kerberos/var/krb5kdc/krb5.conf";

    Configuration hadoopConf = new Configuration();
    LoginUtil.login(userPrincipal, userKeytabPath, krb5ConfPath, hadoopConf);

    String checkPointDir = args[0];
    String batchTime = args[1];
    final String windowTime = args[2];
    String topics = args[3];
    String brokers = args[4];

    Duration batchDuration = Durations.seconds(Integer.parseInt(batchTime));
    Duration windowDuration = Durations.seconds(Integer.parseInt(windowTime));

    // Create a Streaming startup environment.
    SparkConf conf = new SparkConf().setAppName("DataSightStreamingExample");
    JavaStreamingContext jssc = new JavaStreamingContext(conf, batchDuration);

    //Configure the CheckPoint directory for the Streaming. This parameter is mandatory because of existence of the window concept.
    jssc.checkpoint(checkPointDir);

    // Get the list of topic used by kafka
    HashSet<String> topicsSet = new HashSet<String>(Arrays.asList(topics.split(",")));
    HashMap<String, String> kafkaParams = new HashMap<String, String>();
    kafkaParams.put("metadata.broker.list", brokers);

    // Create direct kafka stream with brokers and topics
    // Receive data from the Kafka and generate the corresponding DStream
    JavaDStream<String> lines = KafkaUtils
        .createDirectStream(jssc, String.class, String.class, StringDecoder.class,
            StringDecoder.class, kafkaParams, topicsSet)
        .map(new Function<Tuple2<String, String>, String>() {
          public String call(Tuple2<String, String> tuple2) {
            return tuple2._2();
          }
        });

    // Obtain field properties in each row.
    JavaDStream<Tuple3<String, String, Integer>> records =
        lines.map(new Function<String, Tuple3<String, String, Integer>>() {
          public Tuple3<String, String, Integer> call(String line) throws Exception {
            String[] elems = line.split(",");
            return new Tuple3<String, String, Integer>(elems[0], elems[1], Integer.parseInt(elems[2]));
          }
        });

    //  Filter data about the time that female netizens spend online
    JavaDStream<Tuple2<String, Integer>> femaleRecords =
        records.filter(new Function<Tuple3<String, String, Integer>, Boolean>() {
          public Boolean call(Tuple3<String, String, Integer> line) throws Exception {
            if (line._2().equals("female")) {
              return true;
            } else {
              return false;
            }
          }
        }).map(new Function<Tuple3<String, String, Integer>, Tuple2<String, Integer>>() {
          public Tuple2<String, Integer> call(Tuple3<String, String, Integer> stringStringIntegerTuple3)
              throws Exception {
            return new Tuple2<String, Integer>(stringStringIntegerTuple3._1(), stringStringIntegerTuple3._3());
          }
        });

    // Aggregate the total time that each female netizen spends online within a time frame
    JavaPairDStream<String, Integer> aggregateRecords =
        JavaPairDStream.fromJavaDStream(femaleRecords)
            .reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
              public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
              }
            }, new Function2<Integer, Integer, Integer>() {
              public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer - integer2;
              }
            }, windowDuration, batchDuration);

    // Filter data about users whose consecutive online duration exceeds the threshold
    JavaDStream<Tuple2<String, Integer>> upTimeUser =
        aggregateRecords.filter(new Function<Tuple2<String, Integer>, Boolean>() {
          public Boolean call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
            if (stringIntegerTuple2._2() > 0.9 * Integer.parseInt(windowTime)) {
              return true;
            } else {
              return false;
            }
          }
        }).toJavaDStream();

    // Configure the properties of kafka
    Properties producerConf = new Properties();
    producerConf.put("serializer.class", "kafka.serializer.DefaultEncoder");
    producerConf.put("key.serializer.class", "kafka.serializer.StringEncoder");
    producerConf.put("metadata.broker.list", brokers);
    producerConf.put("request.required.acks", "1");

    // Send the result to the Kafka as a message, which is named as default topic and is randomly sent to a partition.
    JavaDStreamKafkaWriterFactory.fromJavaDStream(upTimeUser).writeToKafka(producerConf, new ProcessingFunc());

    // start Streaming
    jssc.start();
    jssc.awaitTermination();

  }

  static class ProcessingFunc implements Function<Tuple2<String, Integer>, KeyedMessage<String, byte[]>> {
    public KeyedMessage<String, byte[]> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
      String words = stringIntegerTuple2._1() + "," + stringIntegerTuple2._2().toString();
      return new KeyedMessage<String, byte[]>("default", null, words.getBytes());
    }
  }
}
