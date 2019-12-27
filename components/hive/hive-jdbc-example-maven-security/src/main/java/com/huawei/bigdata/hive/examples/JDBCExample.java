package com.huawei.bigdata.hive.examples;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.PropertyConfigurator;

import com.huawei.bigdata.security.LoginUtil;

public class JDBCExample
{
    private static Configuration CONF = null;

    private static String KRB5_FILE = null;

    private static String USER_NAME = null;

    private static String USER_KEYTAB_FILE = null;

    private static String zkQuorum = null;// zookeeper节点ip和端口列表

    private static String auth = null;

    private static String sasl_qop = null;

    private static String zooKeeperNamespace = null;

    private static String serviceDiscoveryMode = null;

    private static String principal = null;
    private static void init() throws IOException
    {
        CONF = new Configuration();

        // zkQuorum获取后的格式为"xxx.xxx.xxx.xxx:24002,xxx.xxx.xxx.xxx:24002,xxx.xxx.xxx.xxx:24002";
        // "xxx.xxx.xxx.xxx"为集群中ZooKeeper所在节点的业务IP，端口默认是24002
        zkQuorum = InitConfResource.zkQuorum;
        auth =InitConfResource.auth;
        sasl_qop =InitConfResource.saslQop;
        zooKeeperNamespace = InitConfResource.zooKeeperNamespace;
        serviceDiscoveryMode = InitConfResource.serviceDiscoveryMode;
        principal = InitConfResource.principal;
        // 设置新建用户的USER_NAME，其中"xxx"指代之前创建的用户名，例如创建的用户为user，则USER_NAME为user
        USER_NAME = InitConfResource.userName;
            // 设置客户端的keytab和krb5文件路径
            USER_KEYTAB_FILE = InitConfResource.userkeytab;
            KRB5_FILE = InitConfResource.userkrb5;
            System.setProperty("java.security.krb5.conf", KRB5_FILE);
            System.setProperty(InitConfResource.ZOOKEEPER_SERVER_PRINCIPAL_KEY, InitConfResource.ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL);

    }

    /**
     * 本示例演示了如何使用Hive JDBC接口来执行HQL命令<br>
     * <br>
     * 
     * @throws ClassNotFoundException
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws SQLException
     * @throws IOException
     */
    public static void main(String[] args)
            throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException, IOException
    {
        PropertyConfigurator.configure(JDBCExample.class.getClassLoader().getResource("conf/log4j.properties").getPath());
        // 参数初始化
        init();

        // 定义HQL，HQL为单条语句，不能包含“;”
        String[] sqls = { "CREATE TABLE IF NOT EXISTS employees_info(id INT,name STRING)",
                "SELECT COUNT(*) FROM employees_info", "DROP TABLE employees_info" };

        // 拼接JDBC URL,如果需要指定访问的数据库，可以在zkQuorum后加上数据库名字(test1为数据库名字，不加的话默认访问default)比如：jdbc:hive2://xxx.xxx.xxx.xxx:24002,xxx.xxx.xxx.xxx:24002,xxx.xxx.xxx.xxx:24002/test1
        //jdbc:hive2://xxx.xxx.xxx.xxx:24002,xxx.xxx.xxx.xxx:24002,xxx.xxx.xxx.xxx:24002/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2;sasl.qop=auth-conf;auth=KERBEROS;principal=hive/hadoop.hadoop.com@HADOOP.COM;
        StringBuilder sBuilder = new StringBuilder("jdbc:hive2://").append(zkQuorum).append("/");
            sBuilder.append(";serviceDiscoveryMode=").append(serviceDiscoveryMode).append(";zooKeeperNamespace=")
                    .append(zooKeeperNamespace).append(";sasl.qop=").append(sasl_qop).append(";auth=").append(auth)
                    .append(";principal=").append(principal).append(";user.principal=").append(USER_NAME)
                    .append(";user.keytab=").append(USER_KEYTAB_FILE).append(";");
        String url = sBuilder.toString();
        System.out.println("-------------------------"+url);
        // 加载Hive JDBC驱动
        Class.forName(InitConfResource.HIVE_DRIVER);

        Connection connection = null;
        try
        {
            // 获取JDBC连接
            // 如果使用的是普通模式，那么第二个参数需要填写正确的用户名，否则会以匿名用户(anonymous)登录
            connection = DriverManager.getConnection(url, "", "");

            // 建表
            // 表建完之后，如果要往表中导数据，可以使用LOAD语句将数据导入表中，比如从HDFS上将数据导入表:
            // load data inpath '/tmp/employees.txt' overwrite into table employees_info;
            execDDL(connection, sqls[0]);
            //154-155设置hive参数，并查看是否设置成功。
            //execDDL(connection,"set hive.exec.compress.output=true");
            //execDML(connection,"set hive.exec.compress.output ");
            System.out.println("Create table success!");

//            // 查询
//            execDML(connection, sqls[1]);
//
//            // 删表
//            execDDL(connection, sqls[2]);
//            System.out.println("Delete table success!");
        }
        catch (Exception e)
        {
            System.out.println("Create connection failed : " + e.getMessage());
        }
        finally
        {
            // 关闭JDBC连接
            if (null != connection)
            {
                connection.close();
            }
        }
    }

    public static void execDDL(Connection connection, String sql) throws SQLException
    {
        //用来存储SQL语句，并使用此对象可以多次有效的执行SQL语句
        PreparedStatement statement = null;
        try
        {
            //创建PreparedStatement用于将参数化SQL语句发送到数据库的对象。
            statement = connection.prepareStatement(sql);
            //在此PreparedStatement对象中执行SQL语句，该语句可以是任何类型的SQL语句。
            statement.execute();
        }
        finally
        {
            if (null != statement)
            {
                statement.close();
            }
        }
    }

    public static void execDML(Connection connection, String sql) throws SQLException
    {
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        ResultSetMetaData resultMetaData = null;

        try
        {
            // 执行HQL
            statement = connection.prepareStatement(sql);
            //在此PreparedStatement对象中执行SQL查询并返回ResultSet查询生成的对象
            resultSet = statement.executeQuery();

            // 输出查询的列名到控制台
            //getMetData 检索此ResultSet对象列的数量，类型和属性。
            resultMetaData = resultSet.getMetaData();
            //getColumnCount():返回此ResultSet对象中的列数。
            int columnCount = resultMetaData.getColumnCount();
            for (int i = 1; i <= columnCount; i++)
            {
                //获取指定列的建议标题，以便在打印输出和显示中使用。
                System.out.print(resultMetaData.getColumnLabel(i) + '\t');
            }
            System.out.println();

            // 输出查询结果到控制台
            while (resultSet.next())
            {
                for (int i = 1; i <= columnCount; i++)
                {
                    System.out.print(resultSet.getString(i) + '\t');
                }
                System.out.println();
            }
        }
        finally
        {
            if (null != resultSet)
            {
                resultSet.close();
            }

            if (null != statement)
            {
                statement.close();
            }
        }
    }

}
