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
import org.apache.hadoop.fs.Path;
import org.apache.log4j.PropertyConfigurator;

import com.huawei.bigdata.security.LoginUtil;

public class JDBCExamplePreLogin
{
    private static final String HIVE_DRIVER = "org.apache.hive.jdbc.HiveDriver";

    private static final String ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME = "Client";

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
    private static  String hiveclientProp =null;
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

        //加载HDFS服务端配置，包含客户端与服务端对接配置
        CONF.addResource(new Path(InitConfResource.hdfsPath));
        CONF.addResource(new Path(InitConfResource.coreSite));

        //需要修改方法中的PRNCIPAL_NAME（用户名）
        //安全模式需要进行kerberos认证，只在系统启动时执行一次。非安全模式可以删除
        //认证相关，安全模式需要，普通模式可以删除
        String PRNCIPAL_NAME =InitConfResource.userName;//需要修改为实际在manager添加的用户
        String KRB5_CONF = InitConfResource.userkrb5;
        String KEY_TAB = InitConfResource.userkeytab;
        LoginUtil.setKrb5Config(KRB5_CONF);
        LoginUtil.setZookeeperServerPrincipal(InitConfResource.ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL);
        LoginUtil.setJaasFile(PRNCIPAL_NAME, KEY_TAB);
        LoginUtil.login(PRNCIPAL_NAME, KEY_TAB, KRB5_CONF, CONF);
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
        PropertyConfigurator.configure(InitConfResource.log4j);
        // 参数初始化
        init();

        // 定义HQL，HQL为单条语句，不能包含“;”
        String[] sqls = { "CREATE TABLE IF NOT EXISTS employees_info(id INT,name STRING)",
                "SELECT COUNT(*) FROM employees_info", "DROP TABLE employees_info" };

        // 拼接JDBC URL
        StringBuilder sBuilder = new StringBuilder("jdbc:hive2://").append(zkQuorum).append("/");
        sBuilder.append(";serviceDiscoveryMode=").append(serviceDiscoveryMode).append(";zooKeeperNamespace=")
                .append(zooKeeperNamespace).append(";sasl.qop=").append(sasl_qop).append(";auth=").append(auth)
                .append(";principal=").append(principal);
        String url = sBuilder.toString();
        System.out.println("-------------------------------------"+url);

        // 加载Hive JDBC驱动
        Class.forName(HIVE_DRIVER);

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
            System.out.println("Create table success!");

            // 查询
            execDML(connection, sqls[1]);

            // 删表
            execDDL(connection, sqls[2]);
            System.out.println("Delete table success!");
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
