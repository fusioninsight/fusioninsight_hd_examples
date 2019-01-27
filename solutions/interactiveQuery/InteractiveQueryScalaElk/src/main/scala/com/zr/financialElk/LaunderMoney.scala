package com.zr.financialElk

import java.io.{File, FileInputStream, InputStream}
import java.sql.{PreparedStatement, ResultSet, SQLException}
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import main.scala.com.zr.utils.{ElkConnection, LoginUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd
import org.apache.spark.{SparkConf, SparkContext}

object LaunderMoney {
  def main(args: Array[String]): Unit = {
    //加载配置
    val propertie = new Properties()
    val in: InputStream = this.getClass.getClassLoader().getResourceAsStream("Fproducer.properties")
    propertie.load(in)
    //安全认证
    val userPrincipal = propertie.getProperty("userPrincipal")
    val userKeytabPath = propertie.getProperty("userKeytabPath")
    val krb5ConfPath = propertie.getProperty("krb5ConfPath")
    val hadoopConf: Configuration = new Configuration()
    LoginUtil.login(userPrincipal, userKeytabPath, krb5ConfPath, hadoopConf)
    //elk用户名和密码
    val userName = propertie.getProperty("userName")
    val pw = propertie.getProperty("password")
    //创建elk连接
    val conn = ElkConnection.getConnection(userName, pw)
    var pst: PreparedStatement = null
    val sql1 = "select bankCount from tradeInfo group by bankCount"
    pst = conn.prepareStatement(sql1)
    val rs: ResultSet = pst.executeQuery()
    //判断洗钱嫌疑账户
    val launderAcount: List[String] = List()
    while (rs.next()) {
      val account: String = rs.getString(1)
      val sql2: String = s"select desBankCount from tradeInfo where bankCount=$account group by desBankCount"
      pst = conn.prepareStatement(sql2)
      val rs2 = pst.executeQuery()
      while (rs2.next()) {
        val account2 = rs2.getString(1)
        if (!account2.equals(account)) {
          val sql3 = s"select desBankCount from tradeInfo where bankCount=$account2 group by desBankCount"
          pst = conn.prepareStatement(sql3)
          val rs3 = pst.executeQuery()
          while (rs3.next()) {
            val account3 = rs3.getString(1)
            if (account3.equals(account)) {
              launderAcount.+:(account)
            } else {
              val sql4 = s"select desBankCount from tradeInfo where bankCount=$account3 group by desBankCount"
              pst = conn.prepareStatement(sql4)
              val rs4 = pst.executeQuery()
              while (rs4.next()) {
                val account4 = rs4.getString(1)
                if (account4.equals(account)) {
                  launderAcount.+:(account)
                } else {
                  val sql5 = s"select desBankCount from tradeInfo where bankCount=$account4 group by desBankCount"
                  pst = conn.prepareStatement(sql5)
                  val rs5 = pst.executeQuery()
                  while (rs5.next()) {
                    val account5 = rs5.getString(1)
                    if (account5.equals(account)) {
                      launderAcount.+:(account)
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    val time = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
    val sqlText = "CREATE TABLE IF NOT EXISTS launderAccount(bankCount VARCHAR(128),recognize VARCHAR(128)) tablespace hdfs;"
    ElkConnection.createTable(conn, sqlText)
    launderAcount.foreach(x => {
      try { //生成预处理语句。
        val time = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
        pst = conn.prepareStatement("INSERT INTO launderAccount VALUES (?,?)")
        pst.setString(1, x.toString)
        pst.setString(2, time)
        pst.addBatch()
        //执行插入操作。
        pst.executeBatch()
        pst.close
      } catch {
        case e: SQLException =>
          if (pst != null) try
            pst.close
          catch {
            case e1: SQLException =>
              e1.printStackTrace()
          }
          e.printStackTrace()
      }
    })
  }
}
