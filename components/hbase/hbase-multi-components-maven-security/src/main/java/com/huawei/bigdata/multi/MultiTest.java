package com.huawei.bigdata.multi;

import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.security.User;
import com.huawei.bigdata.security.LoginUtil;

public class MultiTest {
	
	  private final static Log LOG = LogFactory.getLog(MultiTest.class.getName());

	  private static final String ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME = "Client";
	  private static final String ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL = "zookeeper/hadoop.hadoop.com@HADOOP.COM";

	  private static Configuration HBASE_CONF = null;
	  private static Configuration HDFS_CONF = null;
	  private static String krb5File = null;
	  private static String userName = null;
	  private static String userKeytabFile = null;
	
	public static void main(String[] args) throws Exception{
		try {
			init();
			login();
		} catch (IOException e) {
			LOG.error("Failed to login because ", e);
			return;
		}

		// test hbase
		HBaseSample hbase_example = new HBaseSample(HBASE_CONF);
		hbase_example.test();

		// test HDFS
		HDFSExample hdfs_examples = new HDFSExample(HDFS_CONF);
		hdfs_examples.test();

		// test Hive
		HiveExample hive_example = new HiveExample();
		hive_example.test();

		int i = 0;
		while(i <= 5){
			Thread.sleep(5 * 60 * 1000);
			
			hive_example.test();
			System.out.println("finish to test hive.");

			hbase_example.test();
			System.out.println("finish to test hbase.");
			
			hdfs_examples.test();
			System.out.println("finish to test hdfs.");
			
			i++;
		}
		
		hbase_example.closeConnection();
		hdfs_examples.closeConnection();
		
	}
	  
	private static void login() throws IOException {
	    if (User.isHBaseSecurityEnabled(HBASE_CONF)) {
	      String userdir = System.getProperty("user.dir") + File.separator + "conf" + File.separator;
	      userName = "tester1";
	      userKeytabFile = userdir + "user.keytab";
	      krb5File = userdir + "krb5.conf";

	      /*
	       * if need to connect zk, please provide jaas info about zk. of course,
	       * you can do it as below:
	       * System.setProperty("java.security.auth.login.config", confDirPath +
	       * "jaas.conf"); but the demo can help you more : Note: if this process
	       * will connect more than one zk cluster, the demo may be not proper. you
	       * can contact us for more help
	       */
	      LoginUtil.setJaasConf(ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME, userName, userKeytabFile);
	      LoginUtil.setZookeeperServerPrincipal(ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL);
	      LoginUtil.login(userName, userKeytabFile, krb5File, HBASE_CONF);
	    }
	  }

	  private static void init() throws IOException {
	    // Default load from conf directory
	    HBASE_CONF = new Configuration();
	    String userdir = System.getProperty("user.dir") + File.separator + "conf" + File.separator;
	    HBASE_CONF.addResource(new Path(userdir + "hbase_core-site.xml"));
	    HBASE_CONF.addResource(new Path(userdir + "hbase_hdfs-site.xml"));
	    HBASE_CONF.addResource(new Path(userdir + "hbase-site.xml"));
	    
	    HDFS_CONF =  new Configuration();
	    HDFS_CONF.addResource(new Path(userdir + "hdfs-site.xml"));
	    HDFS_CONF.addResource(new Path(userdir + "core-site.xml"));
	  }

}
