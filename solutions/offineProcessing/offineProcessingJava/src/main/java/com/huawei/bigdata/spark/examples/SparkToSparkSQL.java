package com.huawei.bigdata.spark.examples;

import com.huawei.bigdata.security.LoginUtil;
import com.huawei.bigdata.spark.Body.AdvertisingUserBody;
import com.huawei.bigdata.spark.Body.CouponPushUserBody;
import com.huawei.bigdata.spark.Body.GoldUserBody;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.*;

import java.lang.Boolean;
import java.lang.Double;
import java.lang.Long;
import java.util.List;
public class SparkToSparkSQL
{
//    static
//    {
//        PropertyConfigurator.configure(SparkToSparkSQL.class.getClassLoader().getResource("log4j-executor.properties").getPath());
//    }
//    private final static Log LOG = LogFactory.getLog(SparkToSparkSQL.class.getName());
    public static void main(String[] args) throws Exception
    {
        //用于认证和连接HDFS，如果打包的话，可以删除这个方法
//        UserLogin();

        //创建一个对象用来操作Spark。Spark2.0以后，用SparkSession代替了SparkConf和SparkContext
        //appName是设置应用的名字，方便在Yarn上查找到。
        //master是指定运行方式，如果采用cluster,要删除master,不然报错。
        //如果需要在windows环境上测试，需要加上master如下：
        //SparkSession spark = SparkSession.builder().appName("spark core").master("local").getOrCreate();
        SparkSession spark = SparkSession.builder().appName("spark core").getOrCreate();
        //从HDFS上读取文件，路径为HDFS上的路径
        Dataset dealDataRDD = spark.read().textFile("/hacluster/myfile/shooppingTable.txt");//购物数据:用户ID、商品名称、商品分类、商品金额、购物日期
        String path1 = "/hacluster/myfile/userTable.txt";//HDFS上的位置
        Dataset userDataRDD = spark.read().textFile(path1);//用户信息：用户ID、用户名称、注册时间
        String skimrecordPath="/hacluster/myfile/browsingTable.txt";
        Dataset skimRecordRDD = spark.read().textFile(skimrecordPath);//用户ID、商品名称、浏览时长、浏览日期。
        //将购物的原数据转换为K-V的形式。
        JavaPairRDD<Integer, Tuple4<String ,String,Integer,String>> mapDealDataRDD = MapDealDataRDD( dealDataRDD);
        //将用户的原数据转换成K-V的形式
        JavaPairRDD <Integer,Tuple2<String ,String>> mapUserDataRDD = MapUserDataRDD(userDataRDD);
        //调用方法，获得半年前的时间--转换成了long,方便比较
        final long   beforeTimeMillis = DateToBefor.BeforeTime();
        //进行右连接，把购物信息表和用户信息合在一起，方便进行筛选。
        JavaPairRDD<Integer, Tuple2<Tuple4<String,String,Integer,String>,Optional<Tuple2<String,String>>>> joinDataDU = mapDealDataRDD.leftOuterJoin(mapUserDataRDD);
        //过滤掉购物日期大于半年的数据项，并删除我们不需要的列，这里只留下了（用户ID，用户名，消费金额）
        JavaPairRDD<Tuple2<Integer, String>, Integer> moneyCountInfo = MoneyCountInfo(joinDataDU,beforeTimeMillis);
        //把相同用户ID和用户名的数据项进行累加。便于筛选构建金牌用户表
        JavaPairRDD<Tuple2<Integer ,String>,Integer> countSkimRDD = CountSkimRDD(skimRecordRDD);
        //用于生成金牌用户
        GoldInfo(spark,moneyCountInfo);
        //用户生成优惠券推送表
        DiscountInfo(spark,moneyCountInfo);
        //用于生成广告推广用户
        //4个参数：第一个操作Spark,第二个购物信息表，用来筛选那些浏览了10次但已经买了的购物信息，第三个参数，用于获得浏览次数大于10次的浏览信息，第四个参数用户信息表
        SpreadInfo(spark,mapDealDataRDD,countSkimRDD,mapUserDataRDD);
        Dataset<Row> femaleTimeInfo = spark.sql("select * from DiscountInfo");
        List<String> result = femaleTimeInfo.javaRDD().map(new Function<Row, String>() {
            public String call(Row row) {
                return row.get(2)+","+row.get(3)+","+row.get(0)+","+row.get(1);
            }
        }).collect();
        System.out.println(result.size());
        for(int i =0;i<result.size();i++) {
            System.out.println(result.get(i));
        }
        spark.stop();
     }
    static  void UserLogin()throws Exception{
        //认证代码，方便在本地进行调试。如果打包放到服务器上运行，可以删除
        //conf配置用于链接HDFS
        Configuration conf = new Configuration();

        //加载HDFS服务端配置，包含客户端与服务端对接配置
        conf.addResource(new Path(SparkToSparkSQL.class.getClassLoader().getResource("hdfs-site.xml").getPath()));
        conf.addResource(new Path(SparkToSparkSQL.class.getClassLoader().getResource("core-site.xml").getPath()));

        //需要修改方法中的PRNCIPAL_NAME（用户名）
        //安全模式需要进行kerberos认证，只在系统启动时执行一次。非安全模式可以删除
        if ("kerberos".equalsIgnoreCase(conf.get("hadoop.security.authentication")))
        {
            //认证相关，安全模式需要，普通模式可以删除
            String PRNCIPAL_NAME = "lyysxg";//需要修改为实际在manager添加的用户
            String KRB5_CONF = SparkToSparkSQL.class.getClassLoader().getResource("krb5.conf").getPath();
            String KEY_TAB = SparkToSparkSQL.class.getClassLoader().getResource("user.keytab").getPath();
            System.setProperty("java.security.krb5.conf", KRB5_CONF); //指定kerberos配置文件到JVM
            LoginUtil.login(PRNCIPAL_NAME, KEY_TAB, KRB5_CONF, conf);
        }
    }
    public static JavaPairRDD<Integer, Tuple4<String ,String,Integer,String>> MapDealDataRDD(Dataset dealDataRDD){
        JavaPairRDD<Integer, Tuple4<String ,String,Integer,String>> mapDealDataRDD = dealDataRDD.javaRDD().map(new Function<String, Tuple5<Integer, String, String,Integer,String>>() {
            //先将String类型的数据，转换成对应元数据。这里借助Tuple。
            //Tuple想当于一个容器，原数据有几种数据类型，就调用对应的Tuple.
            //比如：我们例子的购物信息的原数据有5种类型，这里就调用Tuple5。
            //Tuple内的参数和原数据的类型一一对应。
            public Tuple5<Integer, String, String,Integer,String> call(String s) throws Exception {
                //利用逗号，将数据进行拆分。
                String[] tokens = s.split(",");
                //放到容器中。
                Tuple5<Integer, String, String,Integer,String> DealDataRDD = new Tuple5<Integer, String, String,Integer,String>(Integer.parseInt(tokens[0]),tokens[1],tokens[2],Integer.parseInt(tokens[3]),tokens[4]);
                return  DealDataRDD;
            }
            //将元数据转换成K-V的形式，方便后面调用。
            //mapToPair是转换所用的方法，它里面调用了PairFunction方法，返回的是一个JavaPairRDD
            // PairFunction方法里面有2个参数，第一个为需要转换的数据的类型，第二个转换后的数据的数据类型。
        }).mapToPair(new PairFunction<Tuple5<Integer, String, String,Integer,String>, Integer, Tuple4<String ,String,Integer,String>>() {
            //K-Value是2个参数，所有借用Tuple2。
            //value里面有4个参数，所有value调用Tuple4
            public Tuple2<Integer, Tuple4<String ,String,Integer,String>> call(Tuple5<Integer, String, String,Integer,String> DealDataRDD) throws Exception {
                Tuple2<Integer, Tuple4<String ,String,Integer,String>> mapDealDataRDD = new Tuple2<Integer,Tuple4<String ,String,Integer,String>>(DealDataRDD._1(),(new Tuple4<String, String, Integer, String>(DealDataRDD._2(),DealDataRDD._3(),DealDataRDD._4(),DealDataRDD._5())));
                return mapDealDataRDD;
            }
        });
        return mapDealDataRDD;
    }
    static  JavaPairRDD <Integer,Tuple2<String ,String>>  MapUserDataRDD( Dataset userDataRDD){
        //用户ID，用户名，注册时间
        JavaPairRDD <Integer,Tuple2<String ,String>> mapUserDataRDD = userDataRDD.javaRDD().map(new Function<String, Tuple3<Integer,String, String>>() {
            //用户信息原数据有3种类型，所以借助Tuple3容器。
            public Tuple3<Integer,String, String> call(String s) throws Exception {
                //拆分
                String[] tokens = s.split(",");
                Tuple3<Integer,String,String> UserDataRDD = new Tuple3<Integer, String, String>(Integer.parseInt(tokens[0]),tokens[1],tokens[2]);
                return UserDataRDD;
            }
            //转换成K-V形式，调用mapToPair方法。
            //tuple的调用是通过，_位置（）来调用
            //比如：UserDataRDD._1()表示，获得UserDataRDD的第一个参数的值。
        }).mapToPair(new PairFunction<Tuple3<Integer,String,String>,Integer, Tuple2<String, String>>() {
            public Tuple2<Integer,Tuple2<String ,String>> call(Tuple3<Integer,String,String> UserDataRDD) throws Exception {
                Tuple2<Integer,Tuple2<String ,String>> mapUserDataRDD = new Tuple2<Integer,Tuple2<String ,String>>(UserDataRDD._1(),new Tuple2<String, String>(UserDataRDD._2(),UserDataRDD._3()));
                return mapUserDataRDD;
            }
        });
        return  mapUserDataRDD;
    }
    static JavaPairRDD<Tuple2<Integer, String>, Integer> MoneyCountInfo(JavaPairRDD<Integer, Tuple2<Tuple4<String,String,Integer,String>,Optional<Tuple2<String,String>>>> joinDataDU ,final long   beforeTimeMillis) {
        JavaPairRDD<Tuple2<Integer, String>, Integer> moneyCountInfo = joinDataDU.filter(new Function<Tuple2<Integer, Tuple2<Tuple4<String, String, Integer, String>, Optional<Tuple2<String, String>>>>, Boolean>() {
            @Override
            //过滤的话调用的filter方法，它不改变返回类型。过滤的是什么类型，返回的就是什么类型。
            //filter里面调用了Funtion方法，里面有2个参数，第一个为需要过滤的数据的数据类型，第二个返回的boolean值。
            //这里会把不符合条件的数据，自动剔除掉。
            public Boolean call(Tuple2<Integer, Tuple2<Tuple4<String, String, Integer, String>, Optional<Tuple2<String, String>>>> integerTuple2Tuple2) throws Exception {
                //把注册日期为空的，剔除掉。
                boolean IDNotNull;
                if (integerTuple2Tuple2._2._2().equals("")) {
                    IDNotNull = false;
                } else {
                    IDNotNull = true;
                }
                return IDNotNull;
            }
            //将数据转换成元数据：用户ID、商品名称、商品分类、商品金额、购物日期、用户名、注册时间
        }).map(new Function<Tuple2<Integer, Tuple2<Tuple4<String, String, Integer, String>, Optional<Tuple2<String, String>>>>, Tuple7<Integer, String, String, Integer, Long, String, Long>>() {
            @Override
            public Tuple7<Integer, String, String, Integer, Long, String, Long> call(Tuple2<Integer, Tuple2<Tuple4<String, String, Integer, String>, Optional<Tuple2<String, String>>>> integerTuple2Tuple2) throws Exception {
                Tuple7<Integer, String, String, Integer, Long, String, Long> moneyCountInfo1 = new Tuple7<Integer, String, String, Integer, Long, String, Long>(integerTuple2Tuple2._1(), integerTuple2Tuple2._2._1._1(), integerTuple2Tuple2._2._1._2(), integerTuple2Tuple2._2._1._3(), DateToBefor.transformDate(integerTuple2Tuple2._2._1._4()), integerTuple2Tuple2._2._2.get()._1(), DateToBefor.transformDate(integerTuple2Tuple2._2._2.get()._2()));
                return moneyCountInfo1;
            }
            //进行过滤，把半年内为购物的人员剔除掉
        }).filter(new Function<Tuple7<Integer, String, String, Integer, Long, String, Long>, Boolean>() {
            @Override
            public Boolean call(Tuple7<Integer, String, String, Integer, Long, String, Long> integerStringStringIntegerLongStringLongTuple7) throws Exception {
                boolean moreThanHalfYear = integerStringStringIntegerLongStringLongTuple7._5() >= beforeTimeMillis;
                return moreThanHalfYear;
            }
            //将元数据转换成K-V的形式，方便求和。
            //（用户ID，用户名）、消费金额
        }).mapToPair(new PairFunction<Tuple7<Integer, String, String, Integer, Long, String, Long>, Tuple2<Integer, String>, Integer>() {
            @Override
            public Tuple2<Tuple2<Integer, String>, Integer> call(Tuple7<Integer, String, String, Integer, Long, String, Long> integerStringStringIntegerLongStringLongTuple7) throws Exception {
                Tuple2<Tuple2<Integer, String>, Integer> mappairDate = new Tuple2<Tuple2<Integer, String>, Integer>(new Tuple2<Integer, String>(integerStringStringIntegerLongStringLongTuple7._1(), integerStringStringIntegerLongStringLongTuple7._6()), integerStringStringIntegerLongStringLongTuple7._4());
                return mappairDate;
            }
            //reduceBykey方法，是把K相同的项进行求和,返回的是一个JavaPairRDD
            //reduceBykey里面调用了Function2方法。这个方法里面3个参数，第1和第二个位需要需要相加的数据的数据类型，第三个结果的数据类型。
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return (integer + integer2);
            }
        });
        return  moneyCountInfo;
    }
    static void GoldInfo( SparkSession spark,JavaPairRDD<Tuple2<Integer, String>, Integer> moneyCountInfo){
        JavaRDD<GoldUserBody> goldInfo = moneyCountInfo.filter(new Function<Tuple2<Tuple2<Integer, String>, Integer>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Tuple2<Integer, String>, Integer> tuple2IntegerTuple2) throws Exception {
                boolean moreThanThousand = tuple2IntegerTuple2._2 >= 10000;
                return moreThanThousand;
            }
            //将符合条件的信息放到一个对象里面，方便创建表
        }).map(new Function<Tuple2<Tuple2<Integer, String>, Integer>, GoldUserBody>() {
            @Override
            //创建一个存金牌用户的对象
            public GoldUserBody call(Tuple2<Tuple2<Integer, String>, Integer> tuple2IntegerTuple2) throws Exception {
                GoldUserBody goldUserBody = new GoldUserBody();
                goldUserBody.setUserId(tuple2IntegerTuple2._1._1());
                goldUserBody.setUserName(tuple2IntegerTuple2._1._2());
                goldUserBody.setAmount(tuple2IntegerTuple2._2());
                return goldUserBody;
            }
        });
        Dataset<Row> dataFrame = spark.createDataFrame(goldInfo, GoldUserBody.class);
        dataFrame.registerTempTable("GoldInfo");
    }
    static  JavaPairRDD<Tuple2<Integer ,String>,Integer>  CountSkimRDD(Dataset skimRecordRDD){
        JavaPairRDD<Tuple2<Integer ,String>,Integer> countSkimRDD = skimRecordRDD.javaRDD().map(new Function<String, Tuple4<Integer,String,Integer,String>>() {
            public Tuple4<Integer,String,Integer,String> call(String s) throws Exception {
                //拆分
                String[] tokens = s.split(",");
                Tuple4<Integer,String,Integer,String> SkimDataRDD = new Tuple4<Integer,String,Integer,String>(Integer.parseInt(tokens[0]),tokens[1],Integer.parseInt(tokens[2]),tokens[3]);
                return SkimDataRDD;
            }
            //将原数据转化成K-V形式
            //用户ID，用户名，浏览次数
            //这里把所有的浏览次数都设置为1，后面会对浏览次数进行相加
        }).mapToPair(new PairFunction<Tuple4<Integer, String,Integer, String>, Tuple2<Integer, String>, Integer>() {
            @Override
            public Tuple2<Tuple2<Integer, String>,Integer> call(Tuple4<Integer, String,Integer, String> integerStringIntegerStringTuple4) throws Exception {
                Tuple2<Tuple2<Integer,String>,Integer> mapSkimDataRDD = new Tuple2<Tuple2<Integer, String>, Integer>(new Tuple2<Integer, String>(integerStringIntegerStringTuple4._1(),integerStringIntegerStringTuple4._2()),1);
                return mapSkimDataRDD;
            }
            //将相同Key的数据项进行相加，得到浏览次数超过10的用户信息
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
            //过滤，剔除掉浏览次数小于10的用户信息
        }).filter(new Function<Tuple2<Tuple2<Integer, String>, Integer>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Tuple2<Integer, String>, Integer> tuple2IntegerTuple2) throws Exception {
                boolean views = tuple2IntegerTuple2._2>=10;
                return views;
            }
        });
        return  countSkimRDD;
    }
    static void  DiscountInfo(SparkSession spark,JavaPairRDD<Tuple2<Integer, String>, Integer> moneyCountInfo){
        //过滤小消费金额大于1000的用户。
        JavaRDD<CouponPushUserBody> couponPushUserBody = moneyCountInfo.filter(new Function<Tuple2<Tuple2<Integer, String>, Integer>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Tuple2<Integer, String>, Integer> tuple2IntegerTuple2) throws Exception {
                boolean lessThanThousand = tuple2IntegerTuple2._2()<=1000;
                return lessThanThousand;
            }
            //给满足条件的用户打折，并借助对象来构建优惠卷推送用户表
        }).map(new Function<Tuple2<Tuple2<Integer, String>, Integer>, CouponPushUserBody>() {
            @Override
            public CouponPushUserBody call(Tuple2<Tuple2<Integer, String>, Integer> tuple2IntegerTuple2) throws Exception {
                Double money = tuple2IntegerTuple2._2()*0.2;
                if(money <10)
                    money=10.0;
                CouponPushUserBody couponPushUserBody = new CouponPushUserBody();
                couponPushUserBody.setUserID(tuple2IntegerTuple2._1._1());
                couponPushUserBody.setUserName(tuple2IntegerTuple2._1._2());
                couponPushUserBody.setAmount(tuple2IntegerTuple2._2);
                couponPushUserBody.setMoney(money);
                return couponPushUserBody;
            }
        });
        //生一个用户优惠表
    Dataset<Row> dataFrame2 = spark.createDataFrame(couponPushUserBody,CouponPushUserBody.class);
    dataFrame2.registerTempTable("DiscountInfo");
    }
    static void SpreadInfo(SparkSession spark,JavaPairRDD<Integer, Tuple4<String ,String,Integer,String>> mapDealDataRDD,JavaPairRDD<Tuple2<Integer ,String>,Integer> countSkimRDD,JavaPairRDD <Integer,Tuple2<String ,String>> mapUserDataRDD){
        JavaPairRDD<Tuple2<Integer,String>,Tuple3<String,Integer,String>> dealDataKV = mapDealDataRDD.mapToPair(new PairFunction<Tuple2<Integer, Tuple4<String, String, Integer, String>>, Tuple2<Integer, String>, Tuple3<String, Integer, String>>() {
            @Override
            public Tuple2<Tuple2<Integer, String>, Tuple3<String, Integer, String>> call(Tuple2<Integer, Tuple4<String, String, Integer, String>> integerTuple4Tuple2) throws Exception {
                Tuple2<Tuple2<Integer, String>, Tuple3<String, Integer, String>> dealDataKV = new Tuple2<Tuple2<Integer, String>, Tuple3<String, Integer, String>>(new Tuple2<Integer, String>(integerTuple4Tuple2._1(),integerTuple4Tuple2._2._1()),new Tuple3<String, Integer, String>(integerTuple4Tuple2._2._2(),integerTuple4Tuple2._2._3(),integerTuple4Tuple2._2._4()));

                return dealDataKV;
            }
        });
        //右链接转换成K-value的形式：（ID,商品名）、（时长，（分类，价钱，购买时间））
        //以左边的为主，把浏览过10次的商品和购买过的商品进行合并。以便下面筛选
        JavaPairRDD<Tuple2<Integer,String>,Tuple2<Integer,Optional<Tuple3<String,Integer,String>>>> marketJoin1Info = countSkimRDD.leftOuterJoin(dealDataKV);
        //过滤掉已经购买过的商品。
        //Optional可以为空，可以利用isPresent()判空，可以利用get()获得值。
        JavaPairRDD<Integer, String> marketInfo = marketJoin1Info.filter(new Function<Tuple2<Tuple2<Integer, String>, Tuple2<Integer, Optional<Tuple3<String, Integer, String>>>>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Tuple2<Integer, String>, Tuple2<Integer, Optional<Tuple3<String, Integer, String>>>> tuple2Tuple2Tuple2) throws Exception {
                boolean notPurchased = tuple2Tuple2Tuple2._2._2.isPresent()?false:true;
                return  notPurchased;
            }
            //将用户信息转换成K -V形式---用户ID、浏览商品
        }).mapToPair(new PairFunction<Tuple2<Tuple2<Integer, String>, Tuple2<Integer, Optional<Tuple3<String, Integer, String>>>>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<Tuple2<Integer, String>, Tuple2<Integer, Optional<Tuple3<String, Integer, String>>>> tuple2Tuple2Tuple2) throws Exception {
                Tuple2<Integer,String> marketInfo = new Tuple2<Integer, String>(tuple2Tuple2Tuple2._1._1(),tuple2Tuple2Tuple2._1._2());
                return marketInfo;
            }
        });
        //用户ID，用户名，商品名，
        //进行左连接,用户ID、（（用户名，注册日期）、（浏览商品））
        JavaRDD<AdvertisingUserBody> advertisingUserBody = mapUserDataRDD.rightOuterJoin(marketInfo).map(new Function<Tuple2<Integer, Tuple2<Optional<Tuple2<String, String>>, String>>, AdvertisingUserBody>() {
            @Override
            //用户ID、用户名、浏览商品
            public AdvertisingUserBody call(Tuple2<Integer, Tuple2<Optional<Tuple2<String, String>>, String>> integerTuple2Tuple2) throws Exception {
                AdvertisingUserBody advertisingUserBody = new AdvertisingUserBody();
                advertisingUserBody.setUserID(integerTuple2Tuple2._1());
                advertisingUserBody.setUserName(integerTuple2Tuple2._2._1.get()._1());
                advertisingUserBody.setProduct(integerTuple2Tuple2._2._2);
                return advertisingUserBody;
            }
        });
        //创建一个广告推广用户表：用户ID、用户名称、推广商品
        Dataset<Row> dataFrame1 = spark.createDataFrame(advertisingUserBody,AdvertisingUserBody.class);
        dataFrame1.registerTempTable("SpreadInfo");
    }
}
