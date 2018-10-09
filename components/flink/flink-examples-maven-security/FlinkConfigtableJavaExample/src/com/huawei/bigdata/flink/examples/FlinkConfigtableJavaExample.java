package com.huawei.bigdata.flink.examples;

import com.huawei.bigdata.security.LoginUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.*;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/**
 * Read stream data and join from configure table from redis.
 */
public class FlinkConfigtableJavaExample
{

    public static void main(String[] args) throws Exception
    {
        // print comment for command to use run flink
        System.out.println("use command as: \n" + "./bin/flink run --class com.huawei.bigdata.flink.examples.FlinkConfigtableJavaExample" + " -m yarn-cluster -yt /opt/config -yn 3 -yjm 1024 -ytm 1024 " + "/opt/FlinkConfigtableJavaExample.jar --dataPath config/data.txt" + "******************************************************************************************\n" + "Especially you may write following content into config filePath, as in config/read.properties: \n" + "ReadFields=username,age,company,workLocation,educational,workYear,phone,nativeLocation,school\n" + "Redis_Security=true\n" + "Redis_IP_Port=SZV1000064084:22400,SZV1000064082:22400,SZV1000064085:22400\n" + "Redis_Principal=test11@HADOOP.COM\n" + "Redis_KeytabFile=config/user.keytab\n" + "Redis_Krb5File=config/krb5.conf\n" + "******************************************************************************************");

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // get configure and read data and transform to OriginalRecord
        final String dataPath = ParameterTool.fromArgs(args).get("dataPath", "config/data.txt");
        DataStream<OriginalRecord> originalStream = env.readTextFile(dataPath).map(new MapFunction<String, OriginalRecord>()
        {
            public OriginalRecord map(String value) throws Exception
            {
                return getRecord(value);
            }
        }).assignTimestampsAndWatermarks(new Record2TimestampExtractor()).disableChaining();

        // read from redis and join to the whole user information
        AsyncFunction<OriginalRecord, UserRecord> function = new AsyncRedisRequest();
        // timeout set to 2 minutes, max parallel request num set to 5, you can modify this to optimize
        DataStream<UserRecord> result = AsyncDataStream.unorderedWait(originalStream, function, 2, TimeUnit.MINUTES, 5);

        // data transform
        result.filter(new FilterFunction<UserRecord>()
        {
            public boolean filter(UserRecord value) throws Exception
            {
                return value.sexy.equals("female");
            }
        }).keyBy(new UserRecordSelector()).window(TumblingEventTimeWindows.of(Time.seconds(30))).reduce(new ReduceFunction<UserRecord>()
        {
            public UserRecord reduce(UserRecord value1, UserRecord value2) throws Exception
            {
                value1.shoppingTime += value2.shoppingTime;
                return value1;
            }
        }).filter(new FilterFunction<UserRecord>()
        {
            public boolean filter(UserRecord value) throws Exception
            {
                return value.shoppingTime > 120;
            }
        }).print();

        // execute program
        env.execute("FlinkConfigtable java");
    }

    private static class UserRecordSelector implements KeySelector<UserRecord, String>
    {
        public String getKey(UserRecord value) throws Exception
        {
            return value.name;
        }
    }

    // class to set watermark and timestamp
    private static class Record2TimestampExtractor implements AssignerWithPunctuatedWatermarks<OriginalRecord>
    {

        // add tag in the data of datastream elements
        public long extractTimestamp(OriginalRecord element, long previousTimestamp)
        {
            return System.currentTimeMillis();
        }

        // give the watermark to trigger the window to execute, and use the value to check if the window elements is ready
        public Watermark checkAndGetNextWatermark(OriginalRecord element, long extractedTimestamp)
        {
            return new Watermark(extractedTimestamp - 1);
        }
    }

    private static OriginalRecord getRecord(String line)
    {
        String[] elems = line.split(",");
        assert elems.length == 3;
        return new OriginalRecord(elems[0], elems[1], Integer.parseInt(elems[2]));
    }

    public static class OriginalRecord
    {
        private String name;
        private String sexy;
        private int shoppingTime;

        public OriginalRecord(String n, String s, int t)
        {
            name = n;
            sexy = s;
            shoppingTime = t;
        }
    }

    public static class UserRecord
    {
        private String name;
        private int age;
        private String company;
        private String workLocation;
        private String educational;
        private int workYear;
        private String phone;
        private String nativeLocation;
        private String school;
        private String sexy;
        private int shoppingTime;

        public UserRecord(String nm, int a, String c, String w, String e, int wy, String p, String nl, String sc, String sx, int st)
        {
            name = nm;
            age = a;
            company = c;
            workLocation = w;
            educational = e;
            workYear = wy;
            phone = p;
            nativeLocation = nl;
            school = sc;
            sexy = sx;
            shoppingTime = st;
        }

        public void setInput(String input_nm, String input_sx, int input_st)
        {
            name = input_nm;
            sexy = input_sx;
            shoppingTime = input_st;
        }

        public String toString()
        {
            return "UserRecord-----name: " + name + "  age: " + age + "  company: " + company + "  workLocation: " + workLocation + "  educational: " + educational + "  workYear: " + workYear + "  phone: " + phone + "  nativeLocation: " + nativeLocation + "  school: " + school + "  sexy: " + sexy + "  shoppingTime: " + shoppingTime;
        }
    }

    public static class AsyncRedisRequest extends RichAsyncFunction<OriginalRecord, UserRecord>
    {
        private String fields = "";
        private transient JedisCluster client;
        private LoadingCache<String, UserRecord> cacheRecords;

        @Override
        public void open(Configuration parameters) throws Exception
        {
            super.open(parameters);

            // init cache builder
            cacheRecords = CacheBuilder.newBuilder().maximumSize(10000).expireAfterAccess(7, TimeUnit.DAYS).build(new CacheLoader<String, UserRecord>()
            {
                public UserRecord load(String key) throws Exception
                {
                    //load from redis
                    return loadFromRedis(key);
                }
            });

            // get configure from config/read.properties, you must put this with commands:
            // ./bin/yarn-session.sh -t config -n 3 -jm 1024 -tm 1024 or
            // ./bin/flink run -m yarn-cluster -yt config -yn 3 -yjm 1024 -ytm 1024 /opt/test.jar
            String configPath = "config/read.properties";
            fields = ParameterTool.fromPropertiesFile(configPath).get("ReadFields");
            final boolean isSecurity = ParameterTool.fromPropertiesFile(configPath).getBoolean("Redis_Security", true);
            final String hostPort = ParameterTool.fromPropertiesFile(configPath).get("Redis_IP_Port");
            final String principal = ParameterTool.fromPropertiesFile(configPath).get("Redis_Principal");
            final String keytab = ParameterTool.fromPropertiesFile(configPath).get("Redis_KeytabFile");
            final String krb5 = ParameterTool.fromPropertiesFile(configPath).get("Redis_Krb5File");

            // init redis security mode
            System.setProperty("redis.authentication.jaas", isSecurity ? "true" : "false");
            if (System.getProperty("redis.authentication.jaas", "false").equals("true"))
            {
                LoginUtil.setJaasFile(principal, keytab);
                LoginUtil.setKrb5Config(krb5);
                System.setProperty("SERVER_REALM", "hadoop.com");
            }

            // create jedisCluster client
            Set<HostAndPort> hosts = new HashSet<HostAndPort>();
            for (String node : hostPort.split(","))
            {
                hosts.add(new HostAndPort(node.split(":")[0], Integer.parseInt(node.split(":")[1])));
            }
            client = new JedisCluster(hosts, 60000);
            System.out.println("JedisCluster init, getClusterNodes: " + client.getClusterNodes().size());
        }

        @Override
        public void close() throws Exception
        {
            super.close();

            if (client != null)
            {
                System.out.println("JedisCluster close!!!");
                client.close();
            }
        }

        public UserRecord loadFromRedis(final String key) throws Exception
        {
            if (client.getClusterNodes().size() <= 0)
            {
                System.out.println("JedisCluster init failed, getClusterNodes: " + client.getClusterNodes().size());
            }
            if (!client.exists(key))
            {
                System.out.println("test-------cannot find data to key:  " + key);
                return new UserRecord("null", 0, "null", "null", "null", 0, "null", "null", "null", "null", 0);
            }
            else
            {
                // get some fields
                List<String> values = client.hmget(key, fields.split(","));
                System.out.println("test-------key: " + key + "  get some fields:  " + values.toString());
                return new UserRecord(values.get(0), Integer.parseInt(values.get(1)), values.get(2), values.get(3), values.get(4), Integer.parseInt(values.get(5)), values.get(6), values.get(7), values.get(8), "null", 0);
            }
        }

        public void asyncInvoke(final OriginalRecord input, final ResultFuture<UserRecord> resultFuture) throws Exception
        {
            // set key string, if you key is more than one column, build your key string with columns
            String key = input.name;
            UserRecord info = cacheRecords.get(key);
            info.setInput(input.name, input.sexy, input.shoppingTime);
            resultFuture.complete(Collections.singletonList(info));
        }
    }
}