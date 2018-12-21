//package com.huawei.bigdata.Hive;
//import com.huawei.bigdata.tools.InfoBean;
//import org.apache.commons.beanutils.BeanUtils;
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
//
//import org.apache.hadoop.io.NullWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.lib.input.FileSplit;
//
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.log4j.PropertyConfigurator;
//
//
//import java.io.IOException;
//import java.text.ParseException;
//import java.util.ArrayList;
//
//public class MapReduceToHive {
//    public static final String PRNCIPAL_NAME = "fwc@HADOOP.COM";//需要修改为实际在manager添加的用户
//    public static final String KRB5_CONF = MapReduceToHive.class.getClassLoader().getResource("krb5.conf").getPath();
//    public static final String KEY_TAB = MapReduceToHive.class.getClassLoader().getResource("user.keytab").getPath();
//    static
//    {
//        PropertyConfigurator.configure(MapReduceToHive.class.getClassLoader().getResource("log4j.properties").getPath());
//    }
//    private final static Log LOG = LogFactory.getLog(MapReduceToHive.class.getName());
//    public static void main(String[] args)throws Exception{
//     Configuration conf = new Configuration();
//    //需要修改方法中的PRNCIPAL_NAME（用户名）
//    //安全模式需要进行kerberos认证，只在系统启动时执行一次。非安全模式可以删除
//
//
//        @SuppressWarnings("deprecation")
//        Job job = new Job(conf, "Collect Female Info");
//        job.setJarByClass(MapReduceToHive.class);
//
//        // Set map and reduce classes to be executed, or specify the map and
//        // reduce classes using configuration files.
//        job.setMapperClass(CollectionMapper.class);
//        job.setReducerClass(CollectionReducer.class);
//        // Set the output type of the job.
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);
//
//        FileInputFormat.addInputPath(job, new Path("/zwl/text.txt"));
//        FileOutputFormat.setOutputPath(job, new Path("/zwl/text1.txt"));
//
//        // Submit the job to a remote environment for execution.
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
//
//    }
//    public static class CollectionMapper extends Mapper<Object, Text, Text, InfoBean>
//    {
////        String delim ;
////        private Text userIDInfro = new Text();
////       //输出必须序列化
////        private IntWritable valueInfo = new IntWritable();
////
////       public void map(Object key, Text value, Context context) throws IOException, InterruptedException
////       {
////            String line = value.toString();
////            String[] data = line.split(",");
////            userIDInfro.set(data[0]);
////            String date =data[4];
////                try
////                {
////                Long time = DateToBefor.TransformDate(date);
////
////                Long dateTime = DateToBefor.BeforeTime();
////
////                 if(time>dateTime){
////                     valueInfo.set(Integer.parseInt(data[3]));
////                     context.write(userIDInfro,valueInfo);
////                 }
////                }catch (ParseException e)
////                {
////                    e.printStackTrace();
////
////                }
//            InfoBean bean = new InfoBean();
//            Text userId = new Text();
//        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
//        {
//            String line = value.toString();
//            FileSplit split =(FileSplit)context.getInputSplit();
//            String name = split.getPath().getName();
//            String id = "";
//            if(name.startsWith("shoopping"))
//            {
//                String[] data = line.split(",");
//                bean.set(Integer.parseInt(data[0]),data[1],data[2],Integer.parseInt(data[3]),data[4],"","",0);
//                id = data[0];
//            }else{
//                String[] data=line.split(",");
//                bean.set(Integer.parseInt(data[0]),"","",0,"",data[1],data[2],1);
//                id=data[0];
//            }
//            userId.set(id);
//            context.write(userId,bean);
//        }
//    }
////    public   void  setup(Context context) throws IOException,InterruptedException
////      {
////             delim = context.getConfiguration().get("log.delimiter",",");
////
////      }
////    }
//        public static class CollectionReducer extends Reducer<Text,InfoBean, InfoBean, NullWritable>
//    {
////        // Total time threshold
////        private int timeThreshold;
////        private IntWritable result = new IntWritable();
////        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
////            int sum = 0;
////            for(IntWritable value :values){
////               sum += value.get();
////            }
////
////            if(sum <timeThreshold){
////                return;
////            }
////            result.set(sum);
////            context.write(key, result);
////        }
////        public void setup(Context context) throws IOException, InterruptedException {
////
////            // Context obtains configuration information.
////            timeThreshold = context.getConfiguration().getInt("log.time.threshold", 10000);
////        }
//        public void reduce(Text key, Iterable<InfoBean> values, Context context) throws IOException, InterruptedException
//        {
//            InfoBean regitBean = new InfoBean();
//            ArrayList<InfoBean> orderBeans = new ArrayList<InfoBean>();
//            for(InfoBean infoBean : values)
//            {
//                if(infoBean.getFlag()==0)
//                {
//                    try
//                    {
//                        BeanUtils.copyProperties(regitBean,infoBean);
//                    }catch (Exception e)
//                    {
//                        e.printStackTrace();
//                    }
//                }else
//                {
//                    InfoBean shoopingBean = new InfoBean();
//                    try
//                    {
//                        BeanUtils.copyProperties(shoopingBean,infoBean);
//                        orderBeans.add(shoopingBean);
//                    }catch (Exception e)
//                    {
//                        e.printStackTrace();
//                    }
//                }
//            }
//            for(InfoBean bean :orderBeans)
//            {
//                bean.setUserName(regitBean.getUserName());
//                bean.setRegistrationDate(regitBean.getRegistrationDate());
//                context.write(bean,NullWritable.get());
//            }
//        }
//    }
//
//}
