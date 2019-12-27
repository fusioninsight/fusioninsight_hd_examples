package com.huawei.bigdata.spark.examples;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.streaming.ProcessingTime;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.*;
import org.apache.carbondata.streaming.parser.CarbonStreamParser;

import com.huawei.hadoop.security.LoginUtil;

public class KafkaToCarbonStream
{
    public static void main(String[] args)
        throws Exception
    {
        createContext(args);
    }

    private static void createContext(String[] args)
        throws Exception
    {
        //获取用户keytab 信息，注意路径
        String userPrincipal = "sparkuser";
        String userKeytabPath = "/opt/FIclient/user.keytab";
        String krb5ConfPath = "/opt/FIclient/KrbClient/kerberos/var/krb5kdc/krb5.conf";

        //登陆认证
        Configuration hadoopConf = new Configuration();
        LoginUtil.login(userPrincipal, userKeytabPath, krb5ConfPath, hadoopConf);

        //获取参数
        String streamTableName = args[0]; //CarbonData 流式表名
        String brokers = args[1];  //kafka brokers 例如：10.90.46.60:21005,10.90.46.61:21005,10.90.46.62:21005
        String topics = args[2];  //kafka topics 名称， 例如：topic02
        Long batchTime = Long.valueOf(args[3]) * 1000; //流式处理的批次时间，单位秒，例如10
        String checkPointDir = args[4];  //流处理checkPoint 地址，hdfs 路径，例如：/tmp/test01

        //创建SparkSession
        SparkSession spark =
            SparkSession.builder().appName("KafkaToCarbondataStream").config(new SparkConf()).getOrCreate();

        StreamingQuery qry = null;
        try
        {
            //从kafka获取数据
            Dataset<String> lines = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", brokers)
                .option("subscribe", topics)
                .load()
                .selectExpr("CAST(value AS STRING)")
                .as(Encoders.STRING());

            //数据写入到CarbonData 流式表
            qry = lines.writeStream()
                .format("carbondata")
                .trigger(new ProcessingTime(batchTime))
                .option("dbName", "default")
                .option("tableName", streamTableName)
                .option("checkpointLocation", checkPointDir)
                .option("bad_records_logger_enable", false)
                .option(CarbonStreamParser.CARBON_STREAM_PARSER, CarbonStreamParser.CARBON_STREAM_PARSER_CSV)
                .start();

            qry.awaitTermination();
        }
        catch (Exception e)
        {
            e.printStackTrace();
            System.out.println("Wrong reading and writing streaming data");
        }
        finally
        {
            qry.stop();
        }
    }
}
