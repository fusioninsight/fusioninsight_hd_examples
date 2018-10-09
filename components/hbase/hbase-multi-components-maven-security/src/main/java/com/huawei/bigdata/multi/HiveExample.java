package com.huawei.bigdata.multi;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class HiveExample {
	/**
	 * 所连接的集群是否为安全版本
	 */
	private static final boolean isSecureVerson = true;

	private static final String HIVE_DRIVER = "org.apache.hive.jdbc.HiveDriver";

	public HiveExample() {

	}

	/**
	 * 本示例演示了如何使用Hive JDBC接口来执行HQL命令<br>
	 * <br>
	 * 
	 * @throws ClassNotFoundException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 * @throws SQLException
	 */
	public static void main(String[] args)
			throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		// 其中，zkQuorum的"xxx.xxx.xxx.xxx"为集群中Zookeeper所在节点的IP，端口默认是24002
		String zkQuorum = "189.132.134.121:24002,189.132.134.152:24002,189.132.134.162:24002";

		// 定义HQL，HQL为单条语句，不能包含“;”
		String[] sqls = { "CREATE TABLE IF NOT EXISTS employees_info(id INT,name STRING)",
				"SELECT COUNT(*) FROM employees_info", "DROP TABLE employees_info" };

		// 拼接JDBC URL
		StringBuilder sBuilder = new StringBuilder("jdbc:hive2://").append(zkQuorum).append("/");

		if (isSecureVerson) {
			// 设置新建用户的userPrincipal，此处填写为带域名的用户名，例如创建的用户为user，域为HADOOP.COM，则其userPrincipal则为user@HADOOP.COM。
			String userPrincipal = "tester1";
			// 设置客户端的keytab文件路径
			String userKeyTab = "conf/user.keytab";

			sBuilder.append(";serviceDiscoveryMode=").append("zooKeeper").append(";zooKeeperNamespace=")
					.append("hiveserver2;sasl.qop=auth-conf;auth=KERBEROS;principal=hive/hadoop.hadoop.com@HADOOP.COM")
					.append(";user.principal=").append(userPrincipal).append(";user.keytab=").append(userKeyTab)
					.append(";");
		} else {
			// 非安全版
			sBuilder.append(";serviceDiscoveryMode=").append("zooKeeper").append(";zooKeeperNamespace=")
					.append("hiveserver2;auth=none");
		}
		String url = sBuilder.toString();

		// 加载Hive JDBC驱动
		Class.forName(HIVE_DRIVER);

		Connection connection = null;
		try {
			// 获取JDBC连接
			connection = DriverManager.getConnection(url, "", "");
			System.out.println("connection:" + connection.hashCode());

			// 建表
			// 表建完之后，如果要往表中导数据，可以使用LOAD语句将数据导入表中，比如从HDFS上将数据导入表:
			// load data inpath '/tmp/employees.txt' overwrite into table
			// employees_info;
			execDDL(connection, sqls[0]);
			System.out.println("Create table success!");

			Connection connection2 = DriverManager.getConnection(url, "", "");
			System.out.println("connection2:" + connection2.hashCode());

			// 查询
			execDML(connection, sqls[1]);

			// 删表
			execDDL(connection, sqls[2]);
			System.out.println("Delete table success!");
		} finally {
			// 关闭JDBC连接
			if (null != connection) {
				connection.close();
			}
		}
	}

	public void test() throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		// 其中，zkQuorum的"xxx.xxx.xxx.xxx"为集群中Zookeeper所在节点的IP，端口默认是24002
		String zkQuorum = "189.132.134.121:24002,189.132.134.152:24002,189.132.134.162:24002";

		// 定义HQL，HQL为单条语句，不能包含“;”
		String[] sqls = { "CREATE TABLE IF NOT EXISTS employees_info(id INT,name STRING)",
				"SELECT COUNT(*) FROM employees_info", "DROP TABLE employees_info" };

		// 拼接JDBC URL
		StringBuilder sBuilder = new StringBuilder("jdbc:hive2://").append(zkQuorum).append("/");

		if (isSecureVerson) {
			// 设置新建用户的userPrincipal，此处填写为带域名的用户名，例如创建的用户为user，域为HADOOP.COM，则其userPrincipal则为user@HADOOP.COM。
			String userPrincipal = "tester1";
			// 设置客户端的keytab文件路径
			String userKeyTab = "conf/user.keytab";

			sBuilder.append(";serviceDiscoveryMode=").append("zooKeeper").append(";zooKeeperNamespace=")
					.append("hiveserver2;sasl.qop=auth-conf;auth=KERBEROS;principal=hive/hadoop.hadoop.com@HADOOP.COM")
					.append(";user.principal=").append(userPrincipal).append(";user.keytab=").append(userKeyTab)
					.append(";");
		} else {
			// 非安全版
			sBuilder.append(";serviceDiscoveryMode=").append("zooKeeper").append(";zooKeeperNamespace=")
					.append("hiveserver2;auth=none");
		}
		String url = sBuilder.toString();

		// 加载Hive JDBC驱动
		Class.forName(HIVE_DRIVER);

		Connection connection = null;
		try {
			// 获取JDBC连接
			connection = DriverManager.getConnection(url, "", "");
			System.out.println("connection:" + connection.hashCode());

			// 建表
			// 表建完之后，如果要往表中导数据，可以使用LOAD语句将数据导入表中，比如从HDFS上将数据导入表:
			// load data inpath '/tmp/employees.txt' overwrite into table
			// employees_info;
			execDDL(connection, sqls[0]);
			System.out.println("Create table success!");
			
			// 查询
			// execDML(connection,sqls[1]);

			// 删表
			// execDDL(connection,sqls[2]);
			// System.out.println("Delete table success!");
		} finally {
			// 关闭JDBC连接
			if (null != connection) {
				connection.close();
			}
		}
	}

	public static void execDDL(Connection connection, String sql) throws SQLException {
		PreparedStatement statement = null;
		try {
			statement = connection.prepareStatement(sql);
			statement.execute();
		} finally {
			if (null != statement) {
				statement.close();
			}
		}
	}

	public static void execDML(Connection connection, String sql) throws SQLException {
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		ResultSetMetaData resultMetaData = null;

		try {
			// 执行HQL
			statement = connection.prepareStatement(sql);
			resultSet = statement.executeQuery();

			// 输出查询的列名到控制台
			resultMetaData = resultSet.getMetaData();
			int columnCount = resultMetaData.getColumnCount();
			for (int i = 1; i <= columnCount; i++) {
				System.out.print(resultMetaData.getColumnLabel(i) + '\t');
			}
			System.out.println();

			// 输出查询结果到控制台
			while (resultSet.next()) {
				for (int i = 1; i <= columnCount; i++) {
					System.out.print(resultSet.getString(i) + '\t');
				}
				System.out.println();
			}
		} finally {
			if (null != resultSet) {
				resultSet.close();
			}

			if (null != statement) {
				statement.close();
			}
		}
	}
}
