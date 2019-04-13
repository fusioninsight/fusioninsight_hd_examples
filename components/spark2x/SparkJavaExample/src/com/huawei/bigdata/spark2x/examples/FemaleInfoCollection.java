package com.huawei.bigdata.spark2x.examples;
import com.huawei.bigdata.spark2x.security.LoginUtil;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.PropertyConfigurator;

import org.apache.spark.api.java.JavaPairRDD;

import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.api.java.function.Function;

import org.apache.spark.api.java.function.Function2;

import org.apache.spark.api.java.function.PairFunction;

import org.apache.commons.logging.Log;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import scala.Tuple3;
public class FemaleInfoCollection
{
    static
    {
 PropertyConfigurator.configure(FemaleInfoCollection.class.getClassLoader().getResource("log4j-executor.properties").getPath());
    }
    private final static Log LOG = LogFactory.getLog(FemaleInfoCollection.class.getName());
    public static void main(String[] args) throws Exception

    {
        //认证代码，方便在本地进行调试
        //打包放到服务器上运行的话，可以把认证代码删掉
        //加载HDFS服务端配置，包含客户端与服务端对接配置
        Configuration conf = new Configuration();

        //加载HDFS服务端配置，包含客户端与服务端对接配置
        conf.addResource(new Path(FemaleInfoCollection.class.getClassLoader().getResource("hdfs-site.xml").getPath()));
        conf.addResource(new Path(FemaleInfoCollection.class.getClassLoader().getResource("core-site.xml").getPath()));

        //需要修改方法中的PRNCIPAL_NAME（用户名）
        //安全模式需要进行kerberos认证，只在系统启动时执行一次。非安全模式可以删除
        if ("kerberos".equalsIgnoreCase(conf.get("hadoop.security.authentication")))
        {
            //认证相关，安全模式需要，普通模式可以删除
            String PRNCIPAL_NAME = "panel";//需要修改为实际在manager添加的用户
            String KRB5_CONF = FemaleInfoCollection.class.getClassLoader().getResource("krb5.conf").getPath();
            String KEY_TAB = FemaleInfoCollection.class.getClassLoader().getResource("user.keytab").getPath();
            System.setProperty("java.security.krb5.conf", KRB5_CONF); //指定kerberos配置文件到JVM
            LoginUtil.login(PRNCIPAL_NAME, KEY_TAB, KRB5_CONF, conf);
        }
        /******* 以上是认证代码************/
        //创建一个对象，用于配置Spark的一些设置
        SparkSession spark = SparkSession.builder().master("local").appName("spark core").getOrCreate();
        //加载所要进行计算的文件，这里的路径为HDFS上的路径
        Dataset<String> data = spark.read().textFile("/myfile/date");


        //将原数据转换成元组的形式
        //这里调用了Transformation算子的map方法，Map算子里面调用了Fuction方法、
        //Fuction方法有2个参数，第一个为需要转换的文件的类型，第二个转换后的类型
        JavaRDD<Tuple3<String,String,Integer>> person = data.javaRDD().map(new Function<String,Tuple3<String,String,Integer>>()
        {
            private static final long serialVersionUID = -2381522520231963249L;
            @Override
            public Tuple3<String, String, Integer> call(String s) throws Exception
            {
                //对数据进行拆分
                String[] tokens = s.split(",");
                Tuple3<String, String, Integer> person = new Tuple3<String, String, Integer>(tokens[0], tokens[1], Integer.parseInt(tokens[2]));
                return person;

            }

        });
        //过滤，把男性网民的信息过滤掉
        //这里调用了Transformation算子的filter方法。x
        //filter算子里面有2个参数，第一个需要过滤的参数的类型，第二个为一个boolean类型的，他会自动去掉不符合条件的信息
        JavaRDD<Tuple3<String,String,Integer>> female = person.filter(new Function<Tuple3<String,String,Integer>, Boolean>()
        {
            private static final long serialVersionUID = -4210609503909770492L;



            @Override

            public Boolean call(Tuple3<String, String, Integer> person) throws Exception

            {


				Boolean isFemale = person._2().equals("female");

                return isFemale;

            }

        });
        //将数据转换成K-V的形式，方便后面的计算用。
        //这里调用了Transformation算子的mapToPair方法，算子里面调用的PairFuction方法
        //这个方法里面有3个参数，第一个为需要转换的参数的类型，第二个为Key的类型，第三个为value的类型
        JavaPairRDD<String, Integer> females = female.mapToPair(new PairFunction<Tuple3<String, String, Integer>, String, Integer>()
        {
            private static final long serialVersionUID = 8313245377656164868L;
           @Override
            public Tuple2<String, Integer> call(Tuple3<String, String, Integer> female) throws Exception
            {

                //对Tuple对象，我们可以利用_1()的形式调用对应的值。
                //_1()表示第一个参数的值
				Tuple2<String, Integer> femaleAndTime = new  Tuple2<String, Integer>(female._1(), female._3());
                return femaleAndTime;

            }

        });
        //把相同key的记录项进行累加，这里调用了Transformation算子的reduceByKey方法。
        //这个算子调用了Function2方法，这个方法里面有3个参数，第一个，第二个位加数的类型，第三个为结果的类型
        JavaPairRDD<String, Integer> femaleTime = females.reduceByKey(new Function2<Integer, Integer, Integer>()
        {
            private static final long serialVersionUID = -3271456048413349559L;
           @Override
            public Integer call(Integer integer, Integer integer2) throws Exception
            {

                // Sum two online time durations of the same female netizen.
				return (integer + integer2);

            }

        });

        //进行过滤，把上网时长小于2个小时网民信息去掉
		JavaPairRDD<String, Integer> rightFemales = femaleTime.filter(new Function<Tuple2<String, Integer>, Boolean>()
        {
            private static final long serialVersionUID = -3178168214712105171L;
            @Override
            public Boolean call(Tuple2<String, Integer> s) throws Exception
            {
				if(s._2() > (2 * 60))
                {
                    return true;

                }

                return false;

            }

        });
        //最后调用action算子collect方法，把所有符合条件的数据项，通过for循环遍历出来。
        for(Tuple2<String, Integer> d: rightFemales.collect())

        {

            System.out.println(d._1()+","+d._2());

        }

        spark.stop();

    }

}

