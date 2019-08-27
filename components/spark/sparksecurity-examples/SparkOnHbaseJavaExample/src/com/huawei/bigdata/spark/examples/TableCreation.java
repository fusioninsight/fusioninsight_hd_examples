package com.huawei.bigdata.spark.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.huawei.hadoop.security.LoginUtil;
import com.huawei.hadoop.security.KerberosUtil;

/**
 * Create table in hbase.
 */
public class TableCreation {
  public static void main(String[] args) throws IOException {
    String userPrincipal = "sparkuser";
    String userKeytabPath = "/opt/FIclient/user.keytab";
    String krb5ConfPath = "/opt/FIclient/KrbClient/kerberos/var/krb5kdc/krb5.conf";
    String principalName = KerberosUtil.getKrb5DomainRealm();
    String ZKServerPrincipal = "zookeeper/hadoop." + principalName;

    String ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME = "Client";
    String ZOOKEEPER_SERVER_PRINCIPAL_KEY = "zookeeper.server.principal";

    Configuration hadoopConf = new Configuration();
    LoginUtil.setJaasConf(ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME, userPrincipal, userKeytabPath);
    LoginUtil.setZookeeperServerPrincipal(ZOOKEEPER_SERVER_PRINCIPAL_KEY, ZKServerPrincipal);
    LoginUtil.login(userPrincipal, userKeytabPath, krb5ConfPath, hadoopConf);

    // Create the configuration parameter to connect the HBase. The hbase-site.xml must be included in the classpath
    SparkConf conf = new SparkConf().setAppName("CollectFemaleInfo");
    JavaSparkContext jsc = new JavaSparkContext(conf);
    Configuration hbConf = HBaseConfiguration.create(jsc.hadoopConfiguration());
    // Create the connection channel to connect the HBase.
    Connection connection = ConnectionFactory.createConnection(hbConf);

    // Declare the description of the table.
    TableName userTable = TableName.valueOf("shb1");
    HTableDescriptor tableDescr = new HTableDescriptor(userTable);
    tableDescr.addFamily(new HColumnDescriptor("info".getBytes()));

    // Create a table.
    System.out.println("Creating table shb1. ");
    Admin admin = connection.getAdmin();
    if (admin.tableExists(userTable)) {
      admin.disableTable(userTable);
      admin.deleteTable(userTable);
    }
    admin.createTable(tableDescr);

    connection.close();
    jsc.stop();
    System.out.println("Done!");
  }
}
