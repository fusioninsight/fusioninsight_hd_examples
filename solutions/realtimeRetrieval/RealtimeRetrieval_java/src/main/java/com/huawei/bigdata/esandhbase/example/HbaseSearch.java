package com.huawei.bigdata.esandhbase.example;

import com.huawei.bigdata.security.LoginUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.PropertyConfigurator;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class HbaseSearch {
    private static final Log LOG = LogFactory.getLog(HbaseSearch.class.getName());
    private static final String ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL = "zookeeper/hadoop.hadoop.com";

    private static TableName tableName = null;
    private static Connection conn = null;
    private static Table table = null;
    private static Properties properties = new Properties();
    static {
        //日志配置文件
        PropertyConfigurator.configure(HbaseSearch.class.getClassLoader().getResource("log4j.properties").getPath());
    }

    public static void main(String[] args) {
        if(conn == null){
            init();
        }
        search("300002");

    }

    public static void search (String id) {
        if(conn == null){
            init();
        }
        LOG.info("-----------Entering HBase test-------------------");
        try
        {
            tableName = TableName.valueOf("testTableName");//修改“testTableName”为实际表名
            table = conn.getTable(tableName);
            //实例化ResultScanner对象,ResultScanner类把扫描操作转换为类似的get操作，它将每一行数据封装成一个Result实例，
            //并将所有的Result实例放入一个迭代器中。next()调用返回了一个单独的Result实例。
            ResultScanner rScanner = null;
            // 声明要查询的表的信息。
            Scan scan = new Scan();
            scan.addFamily(Bytes.toBytes("Basic"));
            scan.addFamily(Bytes.toBytes("OtherInfo"));
            // String queryCondition = properties.getProperty("queryCondition");
            String queryCondition = id;

            //RowFilter(行健过滤器)相关的过滤方法使用:
            //查询rowkey为queryCondition的信息
            RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,new SubstringComparator(queryCondition));
//        提取rowkey以01结尾数据
//        Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,new RegexStringComparator(".*01$"));
//        提取rowkey以123开头的数据
//        Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,new BinaryPrefixComparator("123".getBytes()));
            scan.setFilter(filter);
            scan.setCaching(1000);

            rScanner = table.getScanner(scan);
            // 打印请求结果.
            for (Result r = rScanner.next(); r != null; r = rScanner.next())
            {
                for (Cell cell : r.rawCells())
                {
                    LOG.info(Bytes.toString(CellUtil.cloneRow(cell)) + ":" + Bytes.toString(CellUtil.cloneFamily(cell))
                            + "," + Bytes.toString(CellUtil.cloneQualifier(cell)) + ","
                            + Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
            if (rScanner != null)
            {
                rScanner.close();
            }
            LOG.info("RowFilter successfully.");
        }
        catch (IOException e)
        {
            LOG.error("HBase test failed.", e);
        }
        catch (Exception e)
        {
            LOG.error("HBase test failed.", e);
        }
        finally
        {
            if (table != null)
            {
                try
                {
                    // 关闭table对象
                    table.close();
                }
                catch (IOException e)
                {
                    LOG.error("Failed to close table ", e);
                }
            }

//            if (conn != null)
//            {
//                try
//                {
//                    // 关闭Connection对象。
//                    conn.close();
//                }
//                catch (IOException e)
//                {
//                    LOG.error("Failed to close the Connection ", e);
//                }
//            }
        }

        LOG.info("Exiting test.");
        LOG.info("-----------finish HBase -------------------");
    }

    private static void init(){
        //创建配置文件对象
        Configuration conf = HBaseConfiguration.create();
        try
        {
            //加载HDFS/HBase服务端配置，用于客户端与服务端对接
            conf.addResource(new Path(HbaseSearch.class.getClassLoader().getResource("core-site.xml").getPath()), false);
            conf.addResource(new Path(HbaseSearch.class.getClassLoader().getResource("hdfs-site.xml").getPath()), false);
            conf.addResource(new Path(HbaseSearch.class.getClassLoader().getResource("hbase-site.xml").getPath()), false);
            //加载consumer 配置 文件信息
            properties.load(new FileInputStream(HbaseSearch.class.getClassLoader().getResource("consumer.properties").getPath()));

            //安全登录,请根据实际情况：安全模式需要安全登录；普通模式不需要
            if (User.isHBaseSecurityEnabled(conf))
            {
                String userName = properties.getProperty("userName");//请根据实际情况，修改“fan651”为实际用户名
                String userKeytabFile = HbaseSearch.class.getClassLoader().getResource("user.keytab").getPath();
                String krb5File = HbaseSearch.class.getClassLoader().getResource("krb5.conf").getPath();

                //配置ZooKeeper认证信息。ZooKeeper为HBase集群中各进程提供分布式协作服务
                LoginUtil.setJaasFile(userName,userKeytabFile);
                LoginUtil.setZookeeperServerPrincipal(ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL);
                LoginUtil.login(userName, userKeytabFile, krb5File, conf);
            }
        }
        catch (IOException e)
        {
            LOG.error("Failed to login because ", e);
            return;
        }

        try
        {
            //创建Connection对象,用于连接 HBase服务器 和 ZooKeeper，Connection也提供实例化Admin和Table对象的方法。
            conn = ConnectionFactory.createConnection(conf);
        }
        catch (Exception e)
        {
            LOG.error("Failed to createConnection because ", e);
        }

    }
}
