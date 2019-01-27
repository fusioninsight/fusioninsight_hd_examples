package com.huawei.bigdata.Local;

import com.huawei.bigdata.Hive.DateToBefor;
import com.huawei.bigdata.Hive.MapReduceToHivePromotion;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
/**
 * 概述：
 * 1)本示例演示了如何使用HCatalog提供的HCatInputFormat和HCatOutputFormat接口。
 * 2)读取MR的输出结果，并存储到Hive中。
 *
 * 使用前准备：
 * 1)先运行LocalRunner。（可以在本地运行）
 *
 * 2)将工程打成jar包后上传。
 * 3)配置环境变量：
 *
 *  export HADOOP_HOME=<path_to_hadoop_install>
 *  export HCAT_HOME=<path_to_hcat_install>
 *  export HIVE_HOME=<path_to_hive_install>
 *  export LIB_JARS=$HCAT_HOME/share/hcatalog/hive-hcatalog-core-1.3.0.jar,
 *         $HCAT_HOME/lib/hive-metastore-1.3.0.jar,
 *         $HCAT_HOME/lib/hive-exec-1.3.0.jar,
 *         $HCAT_HOME/lib/libfb303-0.9.2.jar,
 *         $HCAT_HOME/lib/slf4j-api-1.7.5.jar
 *  export HADOOP_CLASSPATH=$HCAT_HOME/share/hcatalog/hive-hcatalog-core-1.3.0.jar:
 *         $HCAT_HOME/lib/hive-metastore-1.3.0.jar:$HIVE_HOME/lib/hive-exec-1.3.0.jar:
 *         $HCAT_HOME/lib/libfb303-0.9.2.jar:
 *         $HADOOP_HOME/etc/hadoop:$HIVE_HOME/conf:
 *         $HCAT_HOME/lib/slf4j-api-1.7.5.jar
 *
 * 提交任务：
 *利用beeline,进入客户端。
 *         yarn --config $HADOOP_HOME/etc/hadoop jar jar <path_to_jar> <main_class> -libjars $LIB_JARS
 *
 * 参考资料：
 * 详细接口说明请参考：https://cwiki.apache.org/confluence/display/Hive/HCatalog+InputOutput
 *
 * */

public class MapReduceToHive extends Configured implements Tool {
    private final static Log LOG = LogFactory.getLog(MapReduceToHive.class);
    public static void main(String[] args)throws Exception{
        int exitCode = ToolRunner.run(new MapReduceToHive(), args);
        System.exit(exitCode);
    }

    public int run(String[] strings) throws Exception {
        HiveConf.setLoadMetastoreConfig(true);
        Configuration conf = getConf();
        conf.addResource(LocalRunner.class.getClassLoader().getResourceAsStream("user-mapred.xml"));
        String inputPath = conf.get("user.client.mapred.input");
        System.out.println(inputPath);
        String glodOutputPath = conf.get("glodUser.client.mapred.output");
        System.out.println(glodOutputPath);
        String promoteOutputPath = conf.get("promoteUser.client.mapred.output");
        String gloldresult =conf.get("glodUser.client.mapred.output.result");
        String promoteOutputResult =conf.get("promoteUser.client.mapred.result");
        System.out.println(promoteOutputPath);
        String dir = System.getProperty("user.dir");

        Job job1 = new Job(conf, "GroupByDemo");
        job1.setJarByClass(LocalRunner.class);
        job1.setMapperClass(Map.class);
        job1.setReducerClass(ResultReduce.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(WritableComparable.class);
        job1.setOutputValueClass(DefaultHCatRecord.class);
        job1.setOutputFormatClass(HCatOutputFormat.class);
        FileInputFormat.addInputPath(job1, new Path(glodOutputPath));
        System.out.println("写入数据");
        OutputJobInfo outputjobInfo = OutputJobInfo.create("default","goldusers", null);
        HCatOutputFormat.setOutput(job1, outputjobInfo);
        HCatSchema schema = outputjobInfo.getOutputSchema();
        HCatOutputFormat.setSchema(job1, schema);
        return  job1.waitForCompletion(true)?0:1;
    }
    public static class Map extends Mapper<Object,Text,Text,Text>{
        String delim;
        private Text nameInfo = new Text();

        // Output <key,value> must be serialized.
        private Text timeInfo = new Text();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String line = value.toString();
            String[] data = line.split(",");
            String shooppingdate =data[4];
            if(!shooppingdate.equalsIgnoreCase(""))
            {
                try
                {
                    long time = DateToBefor.TransformDate(data[4]);
                    long halfAyearData=DateToBefor.BeforeTime();
                    if(time>halfAyearData)
                    {
                        String userId = data[0];
                        String userName = data[5];
                        nameInfo.set(userId);
                        timeInfo.set(userName+","+data[3]);
                        context.write(nameInfo,timeInfo);
                        LOG.info("-----------写入数据----------");
                    }
                }catch (ParseException e)
                {
                    e.printStackTrace();
                }
            }
        }
        public void setup(Context context) throws IOException, InterruptedException
        {
            // Obtain configuration information using Context.
            delim = context.getConfiguration().get("log.delimiter", ",");

        }
    }
    public static class ResultReduce extends Reducer<Text,Text,Text,HCatRecord>
    {
        int filterMoney;
        private IntWritable result = new IntWritable();
        private String username= new String();

        public void reduce(Text key,Iterable<Text> values,Context context)throws IOException,InterruptedException
        {
            LOG.info("-----------写入数据----------");
            int sum = 0;
            String userName =null;
            for(Text val : values)
            {
                String[] data = val.toString().split(",");
                int numble = Integer.parseInt(data[1]);
                userName = data[0];
                sum += numble;

            }
            if(sum < filterMoney)
            {
                return;
            }
            System.out.println("----------------------写入数据------------------");
            LOG.info("-----------写入数据----------");
            List columns = new ArrayList(3);
            columns.add(new HCatFieldSchema("userID", HCatFieldSchema.Type.INT, ""));
            columns.add(new HCatFieldSchema("userName", HCatFieldSchema.Type.STRING, ""));
            columns.add(new HCatFieldSchema("money", HCatFieldSchema.Type.INT, ""));
            HCatSchema schema = new HCatSchema(columns);
            HCatRecord record = new DefaultHCatRecord(3);
            record.setInteger("userID",schema,Integer.parseInt(key.toString()));
            record.set("userName",schema,userName);
            record.set("money",schema,sum);
            context.write(null, record);
        }
        public void setup(Context context)throws IOException,InterruptedException
        {
            filterMoney=context.getConfiguration().getInt("log.time.filterMoney",4000);
        }
    }

}
