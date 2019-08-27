package com.huawei.bigdata.spark.examples

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import com.huawei.hadoop.security.LoginUtil
import com.huawei.hadoop.security.KerberosUtil

/**
  * Create table in hbase.
  */
object TableCreation {
  def main(args: Array[String]): Unit = {
    val userPrincipal = "sparkuser";
    val userKeytabPath = "/opt/FIclient/user.keytab";
    val krb5ConfPath = "/opt/FIclient/KrbClient/kerberos/var/krb5kdc/krb5.conf";
    val principalName = KerberosUtil.getKrb5DomainRealm()
    val ZKServerPrincipal = "zookeeper/hadoop." + principalName

    val ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME: String = "Client"
    val ZOOKEEPER_SERVER_PRINCIPAL_KEY: String = "zookeeper.server.principal"
    val hadoopConf: Configuration = new Configuration();
    LoginUtil.setJaasConf(ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME, userPrincipal, userKeytabPath)
    LoginUtil.setZookeeperServerPrincipal(ZOOKEEPER_SERVER_PRINCIPAL_KEY, ZKServerPrincipal)
    LoginUtil.login(userPrincipal, userKeytabPath, krb5ConfPath, hadoopConf);

    // Create the configuration parameter to connect the HBase. The hbase-site.xml must be included in the classpath.
    val conf: SparkConf = new SparkConf
    val sc: SparkContext = new SparkContext(conf)
    val hbConf: Configuration = HBaseConfiguration.create(sc.hadoopConfiguration)

    // Create the connection channel to connect the HBase
    val connection: Connection = ConnectionFactory.createConnection(hbConf)

    // Declare the description of the table
    val userTable = TableName.valueOf("shb1")
    val tableDescr = new HTableDescriptor(userTable)
    tableDescr.addFamily(new HColumnDescriptor("info".getBytes))

    // Create a table
    println("Creating table shb1. ")
    val admin = connection.getAdmin
    if (admin.tableExists(userTable)) {
      admin.disableTable(userTable)
      admin.deleteTable(userTable)
    }
    admin.createTable(tableDescr)

    connection.close()
    sc.stop()
    println("Done!")
  }
}
