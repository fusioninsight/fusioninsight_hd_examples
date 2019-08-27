package main.scala.com.zr.utils

import java.sql.{Connection, PreparedStatement, Statement}
import java.sql.SQLException
import java.sql.DriverManager

object ElkConnection
{

  def getConnection(username: String, passwd: String): Connection =
  {
    System.out.println("Begin get Elk's connection.")
    val driver = "org.postgresql.Driver"
    val sourceURL = "jdbc:postgresql://187.5.89.47:25108/postgres"
    var conn: Connection = null
    try {
      //加载数据库驱动。
      Class.forName(driver)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        return null
    }
    try { //创建数据库连接。
      conn = DriverManager.getConnection(sourceURL, username, passwd)
      System.out.println("get  Elk's connection succeed!")
    } catch {
      case e: Exception =>
        e.printStackTrace()
        return null
    }
    return conn
  }

  //创建表
  def createTable(conn: Connection, sqlText: String): Unit =
  {
    var stmt: Statement = null
    try {
      stmt = conn.createStatement()
      //执行SQL语句。
      val rc = stmt.executeUpdate(sqlText)
      stmt.close
    } catch {
      case e: SQLException =>
        if (stmt != null) try
          stmt.close
        catch {
          case e1: SQLException =>
            e1.printStackTrace()
        }
        e.printStackTrace()
    }
  }

}
