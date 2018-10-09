package com.huawei.fiber.example

import java.sql.{Connection, Statement, ResultSet, DriverManager, SQLException}


object FiberJDBCExample {
  def main(args: Array[String]) {
    //1. Check the input parameters.
    if (args.length != 3) {
      System.err.println("The number of parameter should be three.")
      System.err.println("Usage:  cmd \"jdbc:fiber://fiberconfig=<fiber.xml_path>;defaultDriver=<driver_name>\" <spark_data_path> <hive_data_path>")
      System.exit(-1)
    }
    //2. Parse input parameters.
    //Url:The url for connecting Fiber jdbc.
    //sparkDataPath: The text data path of the tb_spark table in the Example. The text data is loaded into the tb_spark table.
    //hiveDataPath: The text data path of the tb_hive table in the Example. The text data is loaded into the tb_hive table.
    val url: String = args(0)
    val sparkDataPath: String = args(1)
    val hiveDataPath: String = args(2)
    //3. Register FiberDriver
    Class.forName("com.huawei.fiber.FiberDriver")
    var connection: Option[Connection] = None
    var statement: Option[Statement] = None
    var resultSet: Option[ResultSet] = None

    try {
      //4. Create a Fiber connection,create a Statement.
      connection = Some(DriverManager.getConnection(url))
      statement = Some(connection.get.createStatement)

      //1. Switch to the Spark execution engine.
      statement.get.execute("set fiber.execution.engine = spark")
      try {
        //2. Drop tb_spark if it exists.
        statement.get.execute("DROP TABLE IF EXISTS tb_spark")
        //3. Create table tb_spark.
        statement.get.execute("CREATE TABLE IF NOT EXISTS tb_spark(Id INT, Name String, Company String)" +
          "row format delimited fields terminated by \",\" STORED AS TEXTFILE")
        statement.get.execute("load data inpath '" + sparkDataPath + "' overwrite into table tb_spark")
        //4. Count the records of tb_spark.
        statement.get.execute("select count(*) from tb_spark")
        resultSet = Some(statement.get.getResultSet)
        println("---------count of tb_spark--------")
        println(" | count |")
        while (resultSet.get.next()) {
          val cnt = resultSet.get.getLong(1)
          println(" | " + cnt + " | ")
        }
        //5. Query the tb_spark.
        statement.get.execute("select * from tb_spark")
        resultSet = Some(statement.get.getResultSet)
        println("---------content  of tb_spark--------")
        println(" | id | name | company |")
        while (resultSet.get.next()) {
          val id = resultSet.get.getInt(1)
          val name = resultSet.get.getString(2)
          val company = resultSet.get.getString(3)
          println(" | " + id + " | " + name + " | " + company + " | ")
        }
      } finally {
        statement.get.execute("DROP TABLE IF EXISTS tb_spark")
      }

      //1. Switch to the Hive execution engine
      statement.get.execute("set fiber.execution.engine = hive")
      try {
        //2. Drop tb_hive if it is exist
        statement.get.execute("DROP TABLE IF EXISTS tb_hive")
        //3. Create table tb_hive
        statement.get.execute("CREATE TABLE IF NOT EXISTS tb_hive(Id INT, Name String, Company String)" +
          "row format delimited fields terminated by \",\" STORED AS TEXTFILE")
        statement.get.execute("load data inpath '" + hiveDataPath + "' overwrite into table tb_hive")
        //4. Count the records of tb_hive
        statement.get.execute("select count(*) from tb_hive")
        resultSet = Some(statement.get.getResultSet)
        println("---------count of tb_hive--------")
        println(" | count |")
        while (resultSet.get.next()) {
          val cnt = resultSet.get.getLong(1)
          println(" | " + cnt + " | ")
        }
        //5. Query the tb_hive
        statement.get.execute("select * from tb_hive")
        resultSet = Some(statement.get.getResultSet)
        println("---------content  of tb_hive--------")
        println(" | id | name | company |")
        while (resultSet.get.next()) {
          val id = resultSet.get.getInt(1)
          val name = resultSet.get.getString(2)
          val company = resultSet.get.getString(3)
          println(" | " + id + " | " + name + " | " + company + " | ")
        }
      } finally {
        statement.get.execute("DROP TABLE IF EXISTS tb_hive")
      }

      //1. Switch to the Phoenix execution engine
      connection.get.setAutoCommit(true)
      statement.get.execute("set fiber.execution.engine = phoenix")
      try {
        //2. Drop tb_phoenix if it is exist
        statement.get.execute("DROP TABLE IF EXISTS tb_phoenix")
        //3. Create table tb_phoenix
        statement.get.execute("CREATE TABLE IF NOT EXISTS tb_phoenix(Id INTEGER not null primary key, Name VARCHAR(20), Company VARCHAR(20))")
        statement.get.execute("UPSERT into tb_phoenix(Id,Name,Company) values (101,'phoenix_user1','company1')");
        statement.get.execute("UPSERT into tb_phoenix(Id,Name,Company) values (102,'phoenix_user2','company2')");
        statement.get.execute("UPSERT into tb_phoenix(Id,Name,Company) values (103,'phoenix_user3','company3')");
        //4. Count the records of tb_phoenix
        statement.get.execute("select count(*) from tb_phoenix")
        resultSet = Some(statement.get.getResultSet)
        println("---------count of tb_phoenix--------")
        println(" | count |")
        while (resultSet.get.next()) {
          val cnt = resultSet.get.getLong(1)
          println(" | " + cnt + " |")
        }
        //5. Query the  tb_phoenix
        statement.get.execute("select * from tb_phoenix")
        resultSet = Some(statement.get.getResultSet)
        println("---------content  of tb_phoenix--------")
        println(" | id | name | company |")
        while (resultSet.get.next()) {
          val id = resultSet.get.getInt(1)
          val name = resultSet.get.getString(2)
          val company = resultSet.get.getString(3)
          println(" | " + id + " | " + name + " | " + company + " | ")
        }
      } finally {
        statement.get.execute("DROP TABLE IF EXISTS tb_phoenix")
      }
    }
    catch {
      case ex: SQLException => {
        System.out.println("Exception happened when changing engine! " + ex)
      }
    } finally {
      //1. Close result set.
      try {
        if (resultSet != null) {
          resultSet.get.close()
        }
      }
      catch {
        case ex: SQLException => {
          System.out.println("Exception happened when close ResultSet! " + ex)
        }
      }
      //2. Close statement.
      try {
        if (statement != null) {
          statement.get.close()
        }
      }
      catch {
        case ex: SQLException => {
          System.out.println("Exception happened when close Statement! " + ex)
        }
      }
      //3. Close connection.
      try {
        if (connection != null) {
          connection.get.close()
        }
      }
      catch {
        case ex: SQLException => {
          System.out.println("Exception happened when close Connection! " + ex)
        }
      }
    }
  }
}
