package com.huawei.bigdata.Local;

import com.huawei.bigdata.Hive.MapReduceToHivePromotion;
import com.huawei.bigdata.tools.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.huawei.bigdata.Hive.MapReduceToHivePromotion.CollectionReducer;
import com.huawei.bigdata.Hive.MapReduceToHivePromotion.CollectionMapper;
import com.huawei.bigdata.Hive.MapReduceToHivePromotion.PromotionReducer;
import com.huawei.bigdata.Hive.MapReduceToHivePromotion.PromotionMapper;
import com.huawei.bigdata.Hive.MapReduceToHivePromotion.ResultMap;
import com.huawei.bigdata.Hive.MapReduceToHivePromotion.ResultReduce;
import com.huawei.bigdata.Hive.MapReduceToHivePromotion.PromotionfilterReducer;
import com.huawei.bigdata.Hive.MapReduceToHivePromotion.PromotionfilterMapper;
import com.huawei.bigdata.Hive.MapReduceToHivePromotion.Reduce;

import java.io.File;

public class LocalRunner {
    public static void main(String[] args) throws Exception{
        TarManager.createJar();
        final String PRNCIPAL_NAME = "lyysxg";//需要修改为实际在manager添加的用户
        final String KRB5_CONF = LocalRunner.class.getClassLoader().getResource("krb5.conf").getPath();
        final String KEY_TAB = LocalRunner.class.getClassLoader().getResource("user.keytab").getPath();
        Configuration conf =getConfiguration();
        System.setProperty("java.security.krb5.conf", KRB5_CONF);
        System.setProperty("sun.security.krb5.debug", "true");
        LoginUtil.setZookeeperServerPrincipal("zookeeper/hadoop.hadoop.com");
        LoginUtil.setKrb5Config(KRB5_CONF);
        LoginUtil.login(PRNCIPAL_NAME,KEY_TAB,KRB5_CONF,conf);
        String inputPath = conf.get("user.client.mapred.input");
        System.out.println(inputPath);
        String glodOutputPath = conf.get("glodUser.client.mapred.output");
        System.out.println(glodOutputPath);
        String promoteOutputPath = conf.get("promoteUser.client.mapred.output");
        String gloldresult =conf.get("glodUser.client.mapred.output.result");
        String promoteOutputResult =conf.get("promoteUser.client.mapred.result");
        String offerInfo = conf.get("offer.client.mapred.output.result");
        System.out.println(promoteOutputPath);
        String dir = System.getProperty("user.dir");

        @SuppressWarnings("deprecation")
        Job job = new Job(conf, "Collect Female Info");
        Job job1 = Job.getInstance(conf);
        Job job2 = Job.getInstance(conf);
        Job job3 = Job.getInstance(conf);
        Job job4 = Job.getInstance(conf);
        Job job5 = Job.getInstance(conf);
        // Set excute jar and class
        job.setJar(dir + File.separator + "mapreduce-examples.jar");
        job.setJarByClass(MapReduceToHivePromotion.class);
        //用户注册和购物信息表合并
        job.setMapperClass(CollectionMapper.class);
        job.setReducerClass(CollectionReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(InfoBean.class);

        job.setOutputKeyClass(InfoBean.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileSystem fileSystem =FileSystem.get(conf);
        Path path  = new Path(glodOutputPath);
        if (fileSystem.exists(path))
        {
            fileSystem.delete(path, true);//第二个参数用于指定是否递归删除
        }
        FileOutputFormat.setOutputPath(job, new Path(glodOutputPath));

        if(job.waitForCompletion(true))
        {
            //生成金牌用户
            job1.setJar(dir + File.separator + "mapreduce-examples.jar");
            job1.setJarByClass(MapReduceToHivePromotion.class);
            job1.setMapperClass(ResultMap.class);
            job1.setReducerClass(ResultReduce.class);
            job1.setMapOutputKeyClass(Text.class);
            job1.setMapOutputValueClass(Text.class);
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(job1, new Path(glodOutputPath));
            if(fileSystem.exists(new Path(gloldresult))){
                fileSystem.delete(new Path(gloldresult), true);
            }
            FileOutputFormat.setOutputPath(job1, new Path(gloldresult));
        }
        job2.setJar(dir + File.separator + "mapreduce-examples.jar");
        job2.setJarByClass(MapReduceToHivePromotion.class);
        job2.setMapperClass(PromotionMapper.class);
        job2.setReducerClass(PromotionReducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(PromoteUsersInfo.class);
        job2.setOutputKeyClass(PromoteUsersInfo.class);
        job2.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job2, new Path(inputPath));
        if(fileSystem.exists(new Path(promoteOutputPath))){
            fileSystem.delete(new Path(promoteOutputPath), true);
        }
        FileOutputFormat.setOutputPath(job2, new Path(promoteOutputPath));
        if (job2.waitForCompletion(true))
        {
            job3.setJar(dir + File.separator + "mapreduce-examples.jar");
            job3.setJarByClass(MapReduceToHivePromotion.class);
            job3.setMapperClass(PromotionfilterMapper.class);
            job3.setReducerClass(PromotionfilterReducer.class);
            job3.setMapOutputKeyClass(Text.class);
            job3.setMapOutputValueClass(Text.class);
            job3.setOutputKeyClass(Text.class);
            job3.setOutputValueClass(NullWritable.class);
            FileInputFormat.addInputPath(job3, new Path(promoteOutputPath));
            if (fileSystem.exists(new Path(promoteOutputResult))) {
                fileSystem.delete(new Path(promoteOutputResult), true);
            }
            FileOutputFormat.setOutputPath(job3, new Path(promoteOutputResult));
        }
        job4.setJar(dir + File.separator + "mapreduce-examples.jar");
        job4.setJarByClass(MapReduceToHivePromotion.class);
        job4.setMapperClass(ResultMap.class);
        job4.setReducerClass(Reduce.class);
        job4.setMapOutputKeyClass(Text.class);
        job4.setMapOutputValueClass(Text.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job4, new Path(glodOutputPath));
        if (fileSystem.exists(new Path(offerInfo))) {
            fileSystem.delete(new Path(offerInfo), true);
        }
        FileOutputFormat.setOutputPath(job4, new Path(offerInfo));
        System.exit(job4.waitForCompletion(true)&&job3.waitForCompletion(true)&&job1.waitForCompletion(true)?0:1);
    }

    /**
     * get conf object
     *
     * @return Configuration
     */
    public static Configuration getConfiguration() {
        // Default load from conf directory
        Configuration conf = new Configuration();
        conf.addResource(LocalRunner.class.getClassLoader().getResourceAsStream("core-site.xml"));
        conf.addResource(LocalRunner.class.getClassLoader().getResourceAsStream("yarn-site.xml"));
        conf.addResource(LocalRunner.class.getClassLoader().getResourceAsStream("mapred-site.xml"));
        conf.addResource(LocalRunner.class.getClassLoader().getResourceAsStream("hdfs-site.xml"));
        conf.addResource(LocalRunner.class.getClassLoader().getResourceAsStream("user-mapred.xml"));
        return conf;
    }
}
