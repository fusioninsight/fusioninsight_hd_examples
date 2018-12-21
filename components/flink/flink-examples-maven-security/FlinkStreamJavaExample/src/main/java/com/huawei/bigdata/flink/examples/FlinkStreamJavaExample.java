package com.huawei.bigdata.flink.examples;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;


public class FlinkStreamJavaExample
{
    public static void main(String[] args) throws Exception
    {
        // 打印出执行flink run的参考命令
        System.out.println("use command as: ");
        System.out.println("******************************************************************************************");
        System.out.println("在Linux环境中运行Flink应用程序，需要先启动Flink集群");
        System.out.println("bin/yarn-session.sh -n 3 -jm 1024 -tm 1024");
        System.out.println("******************************************************************************************");
        System.out.println("<filePath> 为文本读取路径，用逗号分隔，每个节点都需要放一份/opt/log1.txt,/opt/log2.txt");
        System.out.println("<windowTime> 为统计数据的窗口跨度,时间单位都是分");
        System.out.println("bin/flink run --class com.huawei.bigdata.flink.examples.FlinkStreamJavaExample" + " /opt/test.jar --filePath /opt/log1.txt,/opt/log2.txt --windowTime 2");

        // 读取文本路径信息，并使用逗号分隔
        final String[] filePaths = ParameterTool.fromArgs(args).get("filePath", "/opt/log1.txt,/opt/log2.txt").split(",");
        //final String[] filePaths = ParameterTool.fromArgs(args).get("filePath", "/D:/fwc/inputPath/log1.txt,/D:/fwc/inputPath/log2.txt").split(",");
        assert filePaths.length > 0;

        //windowTime设置窗口时间大小，默认2分钟一个窗口足够读取文本内的所有数据了
        final int windowTime = ParameterTool.fromArgs(args).getInt("windowTime", 2);

        //StreamExecutionEnvironment 执行流程序的上下文。环境提供了控制作业执行的方法（例如设置并行性或容错/检查点参数）以及与外部世界交互（数据访问）。
        // 构造执行环境，使用eventTime处理窗口数据,操作的并行度1
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // 读取文本数据流
        DataStream<String> unionStream = env.readTextFile(filePaths[0]);
        if (filePaths.length > 1)
        {
            for (int i = 1; i < filePaths.length; i++)
            {
                //Union功能就是在2个或多个DataStream上进行连接，成为一个新的DataStream
                unionStream = unionStream.union(env.readTextFile(filePaths[i]));
            }
        }

        // 数据转换，构造整个数据处理的逻辑，计算并得出结果打印出来
        unionStream.map(new MapFunction<String, UserRecord>()
        {
            @Override
            public UserRecord map(String value) throws Exception
            {
                return getRecord(value);
            }
        }).assignTimestampsAndWatermarks(new Record2TimestampExtractor()).filter(new FilterFunction<UserRecord>()
        {
            @Override
            public boolean filter(UserRecord value) throws Exception
            {
                return value.sexy.equals("female");
            }
        }).keyBy(new UserRecordSelector()).window(TumblingEventTimeWindows.of(Time.minutes(windowTime))).reduce(new ReduceFunction<UserRecord>()
        {
            @Override
            public UserRecord reduce(UserRecord value1, UserRecord value2) throws Exception
            {
                value1.shoppingTime += value2.shoppingTime;
                return value1;
            }
        }).filter(new FilterFunction<UserRecord>()
        {
            @Override
            public boolean filter(UserRecord value) throws Exception
            {
                return value.shoppingTime > 120;
            }
        }).print();

        // 调用execute触发执行
        env.execute("FemaleInfoCollectionPrint java");
    }

    //构造keyBy的关键字作为分组依据
    private static class UserRecordSelector implements KeySelector<UserRecord, Tuple2<String, String>>
    {
        @Override
        public Tuple2<String, String> getKey(UserRecord value) throws Exception
        {
            return Tuple2.of(value.name, value.sexy);
        }
    }

    //解析文本行数据，构造UserRecord数据结构
    private static UserRecord getRecord(String line)
    {
        String[] elems = line.split(",");
        assert elems.length == 3;
        return new UserRecord(elems[0], elems[1], Integer.parseInt(elems[2]));
    }

    //UserRecord数据结构的定义，并重写了toString打印方法
    public static class UserRecord
    {
        private String name;
        private String sexy;
        private int shoppingTime;

        public UserRecord(String n, String s, int t)
        {
            name = n;
            sexy = s;
            shoppingTime = t;
        }

        public String toString()
        {
            return "name: " + name + "  sexy: " + sexy + "  shoppingTime: " + shoppingTime;
        }
    }

    // 构造继承AssignerWithPunctuatedWatermarks的类，用于设置eventTime以及waterMark
    private static class Record2TimestampExtractor implements AssignerWithPunctuatedWatermarks<UserRecord>
    {

        // 在datastream元素的数据中添加标记
        @Override
        public long extractTimestamp(UserRecord element, long previousTimestamp)
        {
            return System.currentTimeMillis();
        }

        //给watermark触发窗口执行，并使用该值检查窗口元素是否准备就绪
        @Override
        public Watermark checkAndGetNextWatermark(UserRecord element, long extractedTimestamp)
        {
            return new Watermark(extractedTimestamp - 1);
        }
    }
}