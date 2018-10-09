package com.huawei.spark.hive

import java.util.{Date, Properties}

import com.huawei.hadoop.security.LoginUtil
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object SparkToHive {

  /**
    * 购物信息类DealData
    * 用户ID：userId
    * 商品名称：goodsName
    * 商品分类：goodsType
    * 商品金额：goodsPrice
    * 购物日期：buyDate
    **/
  //case class DealData(userId: String, goodsName: String, goodsType: String, goodsPrice: Double, buyDateMillis: Long);

  /**
    * 用户数据类UserData
    * 用户ID：userId
    * 用户名：userName
    * 注册时间：postDate
    **/
  //case class UserData(userId: String, userName: String, postDate: Date);

  /**
    * 浏览信息类SkimRecord
    * 用户ID：userId
    * 商品名称：goodsName
    * 浏览时长：skimTime
    * 浏览日期：skimDate
    **/
  //case class SkimRecord(userId: String, goodsName: String, skimTime: Long, skimDate: Date,count:Int);



  def main(args: Array[String]): Unit = {

    //加载属性文件
    val properties = new Properties()
    val in = this.getClass.getClassLoader().getResourceAsStream("ParamsConfoguration.properties")
    properties.load(in)

    //开始用户安全认证


    val hadoopConf: Configuration = new Configuration();
    println("开始执行安全认证")
    LoginUtil.login(properties.getProperty("userPrincipal")
      , properties.getProperty("userKeytabPath")
      , properties.getProperty("krb5ConfPath"), hadoopConf);

    //建立spark启动环境
    val sparkConf = new SparkConf().setAppName("spark core")
    val sc = new SparkContext(sparkConf)


    //通过配置文件加载数据路径


    //加载数据路径
    val checkPointDir = properties.getProperty("checkPoint")
    val dealDataPath = properties.getProperty("dealdatapath")
    val userDataPath = properties.getProperty("userdatapath")
    val skimRecordPath = properties.getProperty("skimrecordpath")


    //加载数据，返回RDD
    val dealDataRDD = sc.textFile(dealDataPath)
    val userDataRDD = sc.textFile(userDataPath)
    val skimRecordRDD = sc.textFile(skimRecordPath)

    //将加载进来的数据构建为需要的数据模型
    val mapDealDataRDD = dealDataRDD.map(_.split(",")).map(x=>(x(0),x(1),x(2),x(3),x(4)))
      .map(x=> (x._1,(x._2,x._3,x._4,x._5)))

    val mapUserDataRDD = userDataRDD.map(_.split(",")).map(x=>(x(0),x(1),x(2)))
      .map(x=>(x._1,(x._2,x._3)))

    val mapSkimDataRDD = skimRecordRDD.map(_.split(","))


    /**
      * 调用时间回溯方法
      * 在构建用户购物金额的信息时,以半年为限
      * 在每次处理数据的时候,将当前系统时间（毫秒）
      * 作为参考时间,作为数据加载进去,时间回溯半年
      * */
    val beforeTimeMillis = DateToBefore.beforeTime()
    val joinDataDU = mapDealDataRDD.leftOuterJoin(mapUserDataRDD)

    val goldpath = properties.getProperty("goldpath")
    val spreadpath = properties.get("spreadpath")
    val discountpath = properties.getProperty("discountpath")



    //购物金额大于1w元标记为金牌用户，根据购物记录推送同类的最新商品
    //(x._1,(x._2,x._3,x._4,x._5,x._6,x._7))
    val moneyCountInfo = joinDataDU.filter(x => x._2._2.isEmpty == false).map(x =>
      (x._1,x._2._1._1,x._2._1._2,x._2._1._3.toDouble,DateToBefore.transformDate(x._2._1._4),
        x._2._2.last._1,x._2._2.last._2))
      //过滤掉在最近半年里没有购物记录的客户
      .filter( x => x._5>=beforeTimeMillis)
      //计算半年里用户的购物总额
      .map( x => (((x._1,x._6),x._4))).reduceByKey(_+_)
      //过滤掉总额小于1W的
      val goldInfo = moneyCountInfo.filter(x => (x._2>=(10000.toDouble)))
      //构建金牌用户信息
      .map(x => (x._1._1,x._1._2,x._2))
      //保存数据
          .saveAsTextFile(s"${goldpath}")


    /**
      *对于用户浏览大于10次但是并未购买的商品推送折扣广告。
      *如果用户浏览记录后产生过相同商品的购物记录则不需要推送。
      */
    //统计用户对每个商品的浏览次数
    val countSkimRDD = mapSkimDataRDD.map(x => (x(0),x(1),x(2),x(3)))
      //筛选出对某商品的浏览次数大于10的
      .map(x => ((x._1,x._2),1)).reduceByKey(_+_).filter(_._2>10)
    val dealDataKV = mapDealDataRDD.map( x => ((x._1,x._2._1),(x._2._2,x._2._3,x._2._4)))
    //和购物信息进行join
    val marketJoin1Info = countSkimRDD.leftOuterJoin(dealDataKV)
    val marketInfo = marketJoin1Info.filter(x => (x._2._2)==None)
      //x.2.2 == None的即为浏览但未购买的信息
      .map(x => (x._1._1,x._1._2))
    val marketJoinInfo2 = mapUserDataRDD.rightOuterJoin(marketInfo)
      .map(x => (x._1,x._2._1.last._1,x._2._2))
          .saveAsTextFile(s"${spreadpath}")


    /**
      * 对于购物金额低于1000元的用户推送优惠券，优惠券金额为已购物金额的20%s，最低为10元
      * */
      //筛选出购物金额小于1000的
      val countMoneyKV = moneyCountInfo.filter(x => x._2<(1000.toDouble))
        //优惠券为已物金额的20%，如果优惠券小于10元，按10元算
        .map(x => (x._1._1,x._1._2,x._2,if (x._2*(0.2)>=(10.toFloat)) x._2*(0.2) else 10 ))
          .saveAsTextFile(s"${discountpath}")


    sc.stop()


    val sparkHive = SparkSession.builder().appName("gg").enableHiveSupport().getOrCreate()
    import sparkHive.sql
    //load data to hive
    //val hiveContext = new HiveContext(sc)
    //import hiveContext.implicits._
    sparkHive.sql("use slw")

    //建立金牌用户信息表
    sparkHive.sql("create table if not exists GoldUser(userid string,goodsname string,countmoney string) " +
      "row format delimited fields terminated by ',' stored as textfile")
    sparkHive.sql(s"load data inpath \'${goldpath}/part-00000\' into table GoldUser")


    /**
      * 根据推广商品进行分区，满足通过对用户推广的商品的批处理
      */
    sparkHive.sql(("create table if not exists Marketing (userid string,username string,makproduct string)" +
      " row format delimited fields terminated by ',' stored as textfile"))
    sparkHive.sql(s"load data inpath \'${spreadpath}/part-00000\' into table Marketing")

    //用户折扣表
    sparkHive.sql("create table if not exists Discount(userid string,username string,countmoney string,discount string) " +
      "row format delimited fields terminated by ',' stored as textfile")
    sparkHive.sql(s"load data inpath \'${discountpath}/part-00000\' into table Discount")
    sparkHive.stop()
  }
}
