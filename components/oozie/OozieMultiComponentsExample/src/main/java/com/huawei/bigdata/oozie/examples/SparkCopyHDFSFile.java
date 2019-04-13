package com.huawei.bigdata.oozie.examples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public final class SparkCopyHDFSFile
{

    public static void main(String[] args) throws Exception
    {

        if (args.length < 2)
        {
            System.err.println("Usage: SparkCopyHDFSFile <file> <file>");
            System.exit(1);
        }

        int length = args.length;

        String fromFile = args[length - 2];
        String toFile = args[length - 1];
        SparkConf sparkConf = new SparkConf().setAppName("SparkCopyHDFSFile");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = ctx.textFile(fromFile);
        lines.saveAsTextFile(toFile);
        System.out.println("Copied file from " + fromFile + " to " + toFile);
        ctx.stop();
    }
}
