package com.huawei.bigdata.oozie.examples;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import com.huawei.bigdata.oozie.security.LoginUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

/**
 * calculate data from hive/hbase,then update to hbase
 */
public class SparkHiveToHBase implements Serializable{
	private static final Logger LOG = Logger.getLogger(SparkHiveToHBase.class);
	
	private static final String ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME = "Client";
	private static final String ZOOKEEPER_SERVER_PRINCIPAL_KEY = "zookeeper.server.principal";
	private static final String ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL = "zookeeper/hadoop";
	
	public static void main(String[] args) throws Exception {
		System.out.println("login user is " + UserGroupInformation.getLoginUser());
		//认证
		login( new Configuration());
		//读写数据
		UserGroupInformation.getLoginUser().doAs(
			new PrivilegedExceptionAction<Void>() {
				public Void run() throws Exception {
					new SparkHiveToHBase().runJob();
					return null;
				}
		});
	}
	
	private static void login(Configuration conf) throws IOException {
		String userPrincipal = "ladmin";
		String userKeytabFile = "/tmp/conf/user.keytab";
		String krb5File = "/tmp/conf/krb5.conf";

		LoginUtil.setJaasConf(ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME,
				userPrincipal, userKeytabFile);
		LoginUtil.setZookeeperServerPrincipal(ZOOKEEPER_SERVER_PRINCIPAL_KEY,
				ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL);
		LoginUtil.login(userPrincipal, userKeytabFile, krb5File, conf);
		
		System.out.println("===========login success======================" + userKeytabFile + "--------" + krb5File);
	}

	private void runJob() throws Exception {
		// Obtain the data in the table through the Spark interface.
		SparkConf conf = new SparkConf().setAppName("SparkHivetoHbase");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		//读Hive表中数据
		HiveContext sqlContext = new HiveContext(jsc);
		DataFrame dataFrame = sqlContext.sql("select name, account from person");

		// Traverse every Partition in the hive table and update the hbase table
		// If less data, you can use rdd.foreach()
		dataFrame.toJavaRDD().foreachPartition(
			new VoidFunction<Iterator<Row>>() {
				public void call(Iterator<Row> iterator) throws Exception {
					hBaseWriter(iterator);
				}
		});

		jsc.stop();
	}

	/**
	 * write to hbase table in exetutor
	 * 
	 * @param iterator
	 *            partition data from hive table
	 */
	private static void hBaseWriter(Iterator<Row> iterator) {
		Table table = null;
		Connection connection = null;
		try {
			// read hbase
			String tableName = "t3";
			String columnFamily = "f1";
			Configuration conf = HBaseConfiguration.create();
			
			//一定要加载这些配置文件，文件从客户端的/opt/client/HBase/hbase/conf路径下获取
			try {
				String coresite = "/tmp/conf/core-site.xml";
				String hbasesite = "/tmp/conf/hbase-site.xml";
				String hdfssite = "/tmp/conf/hdfs-site.xml";
				conf.addResource(new File(coresite).toURI().toURL());
				conf.addResource(new File(hbasesite).toURI().toURL());
				conf.addResource(new File(hdfssite).toURI().toURL());
			} catch (Exception e) {
				e.printStackTrace(System.out);
			}
			
			connection = ConnectionFactory.createConnection(conf);
			table = connection.getTable(TableName.valueOf(tableName));

			List<Put> putList = new ArrayList<Put>();
			while (iterator.hasNext()) {
				Row item = iterator.next();
				//设置rowkey
				Put put = new Put(item.getString(0).getBytes());
				//设置具体的列值
				put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("account"), (item.getInt(1) + "").getBytes());
				// set the put condition
				putList.add(put);
			}

			if (putList.size() > 0) {
				table.put(putList);
			}
		} catch (Exception e) {
			e.printStackTrace(System.out);
		} finally {
			if (table != null) {
				try {
					table.close();
				} catch (IOException e) {
					e.printStackTrace(System.out);
				}
			}
			if (connection != null) {
				try {
					// Close the HBase connection.
					connection.close();
				} catch (IOException e) {
					e.printStackTrace(System.out);
				}
			}
		}
	}
}
