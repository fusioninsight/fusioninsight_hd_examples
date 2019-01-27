package com.huawei.bigdata.Hive;

import com.huawei.bigdata.tools.InfoBean;
import com.huawei.bigdata.tools.PromoteResult;
import com.huawei.bigdata.tools.PromoteUsersInfo;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;

public class MapReduceToHivePromotion {
    private final static Log LOG = LogFactory.getLog(MapReduceToHivePromotion.class.getName());

    public static class PromotionMapper extends Mapper<Object,Text, Text, PromoteUsersInfo>
    {
        PromoteUsersInfo bean = new PromoteUsersInfo();
        Text userId = new Text();
        protected void map (Object key,Text value,Context context) throws IOException,InterruptedException
        {
            String line = value.toString();
            FileSplit split =(FileSplit)context.getInputSplit();
            String name = split.getPath().getName();
            String id ="";
            if(name.startsWith("browsingTable"))
            {
                String[] data = line.split(",");
                bean.set(Integer.parseInt(data[0]),"","",data[1],Integer.parseInt(data[2]),data[3],0);
                id =data[0];
            }else if(name.startsWith("userTable"))
            {
                String[] data = line.split(",");
                bean.set(Integer.parseInt(data[0]),data[1],data[2],"",0,"",1);
                id = data[0];
            }else
            {
                return;
            }
            userId.set(id);
            context.write(userId,bean);
        }
    }
    public static class PromotionReducer extends Reducer<Text,PromoteUsersInfo,PromoteUsersInfo, NullWritable>
    {

        protected void reduce(Text userId, Iterable<PromoteUsersInfo> promoteUsersInfos,Context context) throws IOException,InterruptedException
        {
            PromoteUsersInfo regitBean = new PromoteUsersInfo();
            ArrayList<PromoteUsersInfo> promoteBeans = new ArrayList<PromoteUsersInfo>();
            for(PromoteUsersInfo infobean : promoteUsersInfos)
            {
                if(infobean.getFlag()==1)
                {
                    try
                    {
                        BeanUtils.copyProperties(regitBean, infobean);
                    }catch (Exception e)
                    {
                        e.printStackTrace();
                    }
                }else
                {
                    PromoteUsersInfo viewBean = new PromoteUsersInfo();
                    try
                    {
                        BeanUtils.copyProperties(viewBean,infobean);
                        promoteBeans.add(viewBean);
                    }catch (Exception e)
                    {
                        e.printStackTrace();
                    }
                }
            }

            for(PromoteUsersInfo bean : promoteBeans)
            {
                bean.setUserName(regitBean.getUserName());
                bean.setRegistrationDate(regitBean.getRegistrationDate());
                context.write(bean,NullWritable.get());
            }
        }
    }
    public static class PromotionfilterMapper extends Mapper<Object, Text, Text, Text>
    {
        Text bean = new Text();
        Text userId = new Text();
        public void map(Object key ,Text value, Context context) throws IOException,InterruptedException
        {
            String line = value.toString();
            String[] data = line.split(",");
            bean.set(data[1]+","+data[3]);
            userId.set(data[0]);
            context.write(userId,bean);

        }
    }
    public static class PromotionfilterReducer extends Reducer<Text,Text,Text,NullWritable>
    {
        Text text = new Text();
        public void reduce(Text key,Iterable<Text> beans,Context context) throws IOException,InterruptedException
        {
            int sum =0;
            for(Text t : beans)
            {
                sum = sum+1;
                if (sum>=2) {
                    text.set(key + "," + t);
                    context.write(text, NullWritable.get());
                }
            }

        }

    }
    public static class CollectionMapper extends Mapper<Object, Text, IntWritable, InfoBean>
    {
        InfoBean bean = new InfoBean();
        IntWritable userId = new IntWritable();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            String line = value.toString();
            FileSplit split =(FileSplit)context.getInputSplit();
            String name = split.getPath().getName();
            int id ;
            if(name.startsWith("shooppingTable"))
            {
                String[] data = line.split(",");
                bean.set(Integer.parseInt(data[0]),data[1],data[2],Integer.parseInt(data[3]),data[4],"","",0);
                id = Integer.parseInt(data[0]);
            }else if(name.startsWith("userTable")){
                String[] data=line.split(",");
                bean.set(Integer.parseInt(data[0]),"","",0,"",data[1],data[2],1);
                id=Integer.parseInt(data[0]);
            }else{
                return;
            }
            userId.set(id);
            context.write(userId,bean);
        }
    }
    public static class CollectionReducer extends Reducer<IntWritable,InfoBean,InfoBean,NullWritable>
    {

        public void reduce(IntWritable key,Iterable<InfoBean> beans,Context context) throws IOException,InterruptedException
        {
            InfoBean bean = new InfoBean();
            ArrayList<InfoBean> infoBeans = new ArrayList<InfoBean>();
            for(InfoBean infoBean :beans)
            {
                if(infoBean.getFlag() ==1)
                {
                    try
                    {
                        BeanUtils.copyProperties(bean,infoBean);
                    }catch (Exception e)
                    {
                        e.printStackTrace();
                    }
                }else
                {
                    InfoBean shoopping = new InfoBean();
                    try
                    {
                        BeanUtils.copyProperties(shoopping,infoBean);
                        infoBeans.add(shoopping);
                    }catch (Exception e)
                    {
                        e.printStackTrace();
                    }
                }
            }
            for(InfoBean result : infoBeans)
            {
                result.setUserName(bean.getUserName());
                result.setRegistrationDate(bean.getRegistrationDate());
                context.write(result,NullWritable.get());
            }
        }
    }
    public static  class ResultMap extends Mapper<Object,Text,Text,Text>
    {
        String delim;

        private Text nameInfo = new Text();

        // Output <key,value> must be serialized.
        private Text timeInfo = new Text();
        public  void map(Object key,Text value,Context context) throws  IOException,InterruptedException
        {
            String line = value.toString();
            String[] data = line.split(",");
            String shoopping =data[4];
            if(!shoopping.equalsIgnoreCase(""))
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
                        timeInfo.set(data[5]+","+data[3]);
                        context.write(nameInfo,timeInfo);
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
    public static class ResultReduce extends Reducer<Text,Text,Text,NullWritable>
    {
        int filterMoney;
        private IntWritable result = new IntWritable();
        private String username= new String();
        public void reduce(Text text,Iterable<Text> values,Context context)throws IOException,InterruptedException
        {
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
            result.set(sum);
            text.set(text.toString()+","+userName+","+sum);
            context.write(text,NullWritable.get());
        }
        public void setup(Context context)throws IOException,InterruptedException
        {
            filterMoney=context.getConfiguration().getInt("log.time.filterMoney",4000);
        }
    }
    public static class Reduce extends Reducer<Text,Text,Text,NullWritable> {
        int filterMoney;
        private IntWritable result = new IntWritable();
        private String username = new String();
        double money;

        public void reduce(Text text, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            String userName = null;
            for (Text val : values) {
                String[] data = val.toString().split(",");
                int numble = Integer.parseInt(data[1]);
                userName = data[0];
                sum += numble;

            }
            if (sum < filterMoney) {
                money = sum *0.2;
               if(money<10)
               {
                money =10;
               }
               text.set(text+","+userName+","+sum+","+money);
               context.write(text,NullWritable.get());
            }

        }

        public void setup(Context context) throws IOException, InterruptedException {
            filterMoney = context.getConfiguration().getInt("log.time.filterMoney", 1000);
        }
    }

}
