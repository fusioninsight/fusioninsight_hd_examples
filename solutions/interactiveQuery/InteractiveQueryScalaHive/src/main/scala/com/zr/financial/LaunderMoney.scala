package main.scala.com.zr.financial

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import main.scala.com.zr.utils.LoginUtil
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

//洗钱行为判断
object LaunderMoney {
  def main(args: Array[String]): Unit = {
    //加载配置文件数据
    val propertie = new Properties()
    val in = this.getClass.getClassLoader().getResourceAsStream("Fproducer.properties")
    propertie.load(in)
    //安全认证
    val userPrincipal = propertie.getProperty("userPrincipal")
    val userKeytabPath = propertie.getProperty("userKeytabPath")
    val krb5ConfPath = propertie.getProperty("krb5ConfPath")
    val hadoopConf: Configuration = new Configuration()
    LoginUtil.login(userPrincipal, userKeytabPath, krb5ConfPath, hadoopConf)
    //spark配置
    val conf = new SparkConf().setMaster("local[2]").setAppName("kafka")
    val sc = new SparkContext(conf)
    val sparkSession = SparkSession.builder().appName("launder money").master("local").enableHiveSupport().getOrCreate()
    import sparkSession.sql
    //查出发生交易的账户(付款方)
    val df = sql("select bankCount from tradeInfo group by bankCount")
    val launderAcount: List[String] = List()
    df.foreach(x => {
      //查询每个付款方对应的收款方
      val df2 = sql(s"select desBankCount from tradeInfo where bankCount=$x group by desBankCount")
      df2.foreach(t => {

        if (!t.equals(x)) {
          val df3 = sql(s"select desBankCount from tradeInfo where bankCount=$t group by desBankCount")
          df3.foreach(s => {
            if (s.equals(x)) {
              launderAcount.+:(x)
            } else {
              val df4 = sql(s"select desBankCount from tradeInfo where bankCount=$s group by desBankCount")
              df4.foreach(f => {
                if (f.equals(x)) {
                  launderAcount.+:(x)
                } else {
                  val df5 = sql(s"select desBankCount from tradeInfo where bankCount=$f group by desBankCount")
                  df5.foreach(fi => {
                    if (fi.equals(x)) {
                      launderAcount.+:(x)
                    }
                  })
                }
              })
            }
          })
        }
      })
    })
    val time = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
    val launderPath = propertie.getProperty("launderPath")
    //将具有洗钱嫌疑的账户存入hdfs并导入hive
    val launderRdd = sc.parallelize(launderAcount).map(x => (x, time)).map(x => x._1.toString + "|" + x._2).
      saveAsTextFile(launderPath)
    val createTable = sql("create table if not exists spark_hive.launderAccount(bankCount STRING,recognize STRING) row format delimited fields terminated by '|'")
    val load = sql("load data inpath '/test/launderAccount' overwrite into table launderAccount")

  }
}
