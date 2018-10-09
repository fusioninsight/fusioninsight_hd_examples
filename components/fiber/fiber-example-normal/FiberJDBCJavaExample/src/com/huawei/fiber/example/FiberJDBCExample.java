package com.huawei.fiber.example;

import java.sql.*;

public class FiberJDBCExample {

  public static void main(String[] args) throws Exception {
    //1. Check the input parameters.
    if (args.length != 3) {
      System.err.println("The number of parameter should be three.");
      System.err.println("Usage:  cmd \"jdbc:fiber://fiberconfig=<fiber.xml_path>;defaultDriver=<driver_name>\" <spark_data_path> <hive_data_path>");
      System.exit(-1);
    }
    //2. Parse input parameters.
    //Url:The url for connecting Fiber jdbc.
    //sparkDataPath: The text data path of the tb_spark table in the Example. The text data is loaded into the tb_spark table.
    //hiveDataPath: The text data path of the tb_hive table in the Example. The text data is loaded into the tb_hive table.
    String url = args[0];
    String sparkDataPath = args[1];
    String hiveDataPath = args[2];
    //3. Register FiberDriver
    Class.forName("com.huawei.fiber.FiberDriver");

    Connection conn = null;
    Statement stmt = null;
    ResultSet rs = null;
    try {
      //4. Create a Fiber connection,create a Statement.
      conn = DriverManager.getConnection(url);
      stmt = conn.createStatement();

      //1. Switch to the Spark execution engine.
      stmt.execute("set fiber.execution.engine = spark");
      try {
        //2. Drop tb_spark if it exists.
        stmt.execute("DROP TABLE IF EXISTS tb_spark");
        //3. Create table tb_spark.
        stmt.execute("CREATE TABLE IF NOT EXISTS tb_spark(Id INT, Name String, Company String)" +
          " row format delimited fields terminated by \",\" STORED AS TEXTFILE");
        stmt.execute("load data inpath '" + sparkDataPath + "' overwrite into table tb_spark");
        //4. Count the records of tb_spark.
        stmt.execute("select count(*) from tb_spark");
        rs = stmt.getResultSet();
        System.out.println("--------- count of tb_spark--------");
        System.out.println("| count |");
        while (rs.next()) {
          long cnt = rs.getLong(1);
          System.out.println("| " + cnt + " |");
        }
        //5. Query the tb_spark.
        stmt.execute("select * from tb_spark");
        rs = stmt.getResultSet();
        System.out.println("---------content of tb_spark--------");
        System.out.println("| id | name | company |");
        while (rs.next()) {
          int id = rs.getInt(1);
          String name = rs.getString(2);
          String company = rs.getString(3);
          System.out.println("|  " + id + " | " + name + " | " + company + " | ");
        }
      } catch (SQLException e) {
        System.out.println("Exception happened when engine is spark. " + e.getMessage());
      } finally {
        stmt.execute("DROP TABLE IF EXISTS tb_spark");
      }

      //1. Switch to the Hive execution engine
      stmt.execute("set fiber.execution.engine = hive");
      try {
        //2. Drop tb_hive if it is exist
        stmt.execute("DROP TABLE IF EXISTS tb_hive");
        //3. Create table tb_hive
        stmt.execute("CREATE TABLE IF NOT EXISTS tb_hive(Id INT, Name String, Company String)" +
          " row format delimited fields terminated by \",\" STORED AS TEXTFILE");
        stmt.execute("load data inpath '" + hiveDataPath + "' overwrite into table tb_hive");
        //4. Count the records of tb_hive
        stmt.execute("select count(*) from tb_hive");
        rs = stmt.getResultSet();
        System.out.println("--------- count of tb_hive--------");
        System.out.println("| count |");
        while (rs.next()) {
          long cnt = rs.getLong(1);
          System.out.println("| " + cnt + " |");
        }
        //5. Query the tb_hive
        stmt.execute("select * from tb_hive");
        rs = stmt.getResultSet();
        System.out.println("---------content of tb_hive--------");
        System.out.println("| id | name | company |");
        while (rs.next()) {
          int id = rs.getInt(1);
          String name = rs.getString(2);
          String company = rs.getString(3);
          System.out.println("|  " + id + " | " + name + " | " + company + " | ");
        }
      } catch (SQLException e) {
        System.out.println("Exception happened when engine is hive. " + e.getMessage());
      } finally {
        stmt.execute("DROP TABLE IF EXISTS tb_hive");
      }

      //1. Switch to the Phoenix execution engine
      conn.setAutoCommit(true);
      stmt.execute("set fiber.execution.engine = phoenix");
      try {
        //2. Drop tb_phoenix if it is exist
        stmt.execute("DROP TABLE IF EXISTS tb_phoenix");
        //3. Create table tb_phoenix
        stmt.execute("CREATE TABLE IF NOT EXISTS tb_phoenix(Id INTEGER not null primary key, Name VARCHAR(20), Company VARCHAR(20))");
        stmt.execute("UPSERT into tb_phoenix(Id, Name,Company) values (101,'phoenix_user1','company1')");
        stmt.execute("UPSERT into tb_phoenix(Id, Name,Company) values (102,'phoenix_user2','company2')");
        stmt.execute("UPSERT into tb_phoenix(Id, Name,Company) values (103,'phoenix_user3','company3')");
        //4. Count the records of tb_phoenix
        stmt.execute("select count(*) from tb_phoenix");
        rs = stmt.getResultSet();
        System.out.println("--------- count of tb_phoenix--------");
        System.out.println("| count |");
        while (rs.next()) {
          long cnt = rs.getLong(1);
          System.out.println("| " + cnt + " |");
        }
        //5. Query the  tb_phoenix
        stmt.execute("select * from tb_phoenix");
        rs = stmt.getResultSet();
        System.out.println("---------content of tb_phoenix--------");
        System.out.println("| id | name | company |");
        while (rs.next()) {
          int id = rs.getInt(1);
          String name = rs.getString(2);
          String company = rs.getString(3);
          System.out.println("|  " + id + " | " + name + " | " + company + " | ");
        }
      } catch (SQLException e) {
        System.out.println("Exception happened when engine is phoenix. " + e.getMessage());
      } finally {
        stmt.execute("DROP TABLE IF EXISTS tb_phoenix");
      }
    } catch (SQLException e) {
      System.out.println("Exception happened when changing engine! " + e);
    } finally {
      //1. Close result set.
      try {
        if (null != rs) {
          rs.close();
        }
      } catch (SQLException e) {
        System.out.println("Exception happened when close ResultSet! " + e);
      }
      //2. Close statement.
      try {
        if (null != stmt) {
          stmt.close();
        }
      } catch (SQLException e) {
        System.out.println("Exception happened when close Statement! " + e);
      }
      //3. Close connection.
      try {
        if (null != conn) {
          conn.close();
        }
      } catch (SQLException e) {
        System.out.println("Exception happened when close Connection! " + e);
      }
    }
  }
}