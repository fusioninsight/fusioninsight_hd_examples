package com.huawei.bigdata.hbase.examples;

import com.huawei.bigdata.security.LoginUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.security.User;
import org.apache.log4j.PropertyConfigurator;

import java.io.File;
import java.io.IOException;

public class TestZKSample
{

    private static final Log LOG = LogFactory.getLog(TestZKSample.class.getName());

    private TableName tableName = null;

    private static Configuration conf = null;

    private Connection conn = null;

    private static final String ZOOKEEPER_SERVER_PRINCIPAL_KEY = "zookeeper.server.principal";

    private static final String ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL = "zookeeper/hadoop.hadoop.com";

    public void testSample() throws Exception
    {

        tableName = TableName.valueOf("hbase_zk_sample_table");

        init();
        login();
        conn = ConnectionFactory.createConnection(conf);
        // HBase connect huawei zookeeper
        createTable(conf);
        dropTable();

        // HBase connect apache zookeeper
        connectApacheZK();
    }

    private void connectApacheZK() throws IOException, org.apache.zookeeper.KeeperException
    {
        try
        {
            System.setProperty("zookeeper.sasl.client", "false");
            org.apache.zookeeper.ZooKeeper digestZk = new org.apache.zookeeper.ZooKeeper("127.0.0.1:2181", 60000, null);

            LOG.info("digest directory：\n" + digestZk.getChildren("/", null));

            LOG.info("Successfully connect to apache zookeeper.");
        }
        catch (InterruptedException e)
        {
            LOG.error("Found error when connect apache zookeeper ", e);
        }

    }

    private static void init() throws IOException
    {
        // Default load from conf directory
        conf = HBaseConfiguration.create();
        String userDir = TestZKSample.class.getClassLoader().getResource("conf").getPath() + File.separator;
        conf.addResource(new Path(TestZKSample.class.getClassLoader().getResource("conf/core-site.xml").getPath()), false);
        conf.addResource(new Path(TestZKSample.class.getClassLoader().getResource("conf/hdfs-site.xml").getPath()), false);
        conf.addResource(new Path(TestZKSample.class.getClassLoader().getResource("conf/hbase-site.xml").getPath()), false);
    }

    private static void login() throws IOException
    {
        if (User.isHBaseSecurityEnabled(conf))
        {
            String userName = "hbaseuser1";
            String userKeytabFile = TestZKSample.class.getClassLoader().getResource("conf/user.keytab").getPath();
            String krb5File = TestZKSample.class.getClassLoader().getResource("conf/krb5.conf").getPath();

            /*
             * if need to connect zk, please provide jaas info about zk. of course, you can do it as below:
             * System.setProperty("java.security.auth.login.config", confDirPath + "jaas.conf"); but the demo can help
             * you more : Note: if this process will connect more than one zk cluster, the demo may be not proper. you
             * can contact us for more help
             */
            LoginUtil.setJaasFile(userName,userKeytabFile);
            LoginUtil.setZookeeperServerPrincipal(ZOOKEEPER_SERVER_PRINCIPAL_KEY, ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL);
            LoginUtil.login(userName, userKeytabFile, krb5File, conf);
        }
    }

    /**
     * Create user info table
     */
    public void createTable(Configuration conf)
    {
        LOG.info("Entering testCreateTable.");

        // Specify the table descriptor.
        HTableDescriptor htd = new HTableDescriptor(tableName);

        // Set the column family name to info.
        HColumnDescriptor hcd = new HColumnDescriptor("info");

        // Set data encoding methods，HBase provides DIFF,FAST_DIFF,PREFIX
        // and PREFIX_TREE
        hcd.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);

        // Set compression methods, HBase provides two default compression
        // methods:GZ and SNAPPY
        // GZ has the highest compression rate,but low compression and
        // decompression effeciency,fit for cold data
        // SNAPPY has low compression rate, but high compression and
        // decompression effeciency,fit for hot data.
        // it is advised to use SANPPY
        hcd.setCompressionType(Compression.Algorithm.SNAPPY);
        // hcd.setEncryptionType(Bytes.toBytes("AES"));
        // hcd.setEncryptionKey(Bytes.toBytes("AES"));

        htd.addFamily(hcd);

        Admin admin = null;
        try
        {
            // Instantiate an Admin object.
            admin = conn.getAdmin();
            if (!admin.tableExists(tableName))
            {
                LOG.info("Creating table...");
                admin.createTable(htd);
                LOG.info(admin.getClusterStatus());
                LOG.info(admin.listNamespaceDescriptors());
                LOG.info("Table created successfully.");
            }
            else
            {
                LOG.warn("table already exists");
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
            // LOG.error("Create table failed.", e);
        }
        finally
        {
            if (admin != null)
            {
                try
                {
                    // Close the Admin object.
                    admin.close();
                }
                catch (IOException e)
                {
                    LOG.error("Failed to close admin ", e);
                }
            }
        }
        LOG.info("Exiting testCreateTable.");
    }

    /**
     * Delete user table
     */
    public void dropTable()
    {
        LOG.info("Entering dropTable.");

        Admin admin = null;
        try
        {
            admin = conn.getAdmin();
            if (admin.tableExists(tableName))
            {
                // Disable the table before deleting it.
                admin.disableTable(tableName);

                // Delete table.
                admin.deleteTable(tableName);
            }
            LOG.info("Drop table successfully.");
        }
        catch (IOException e)
        {
            LOG.error("Drop table failed ", e);
        }
        finally
        {
            if (admin != null)
            {
                try
                {
                    // Close the Admin object.
                    admin.close();
                }
                catch (IOException e)
                {
                    LOG.error("Close admin failed ", e);
                }
            }
        }
        LOG.info("Exiting dropTable.");
    }

    public static void main(String[] args)
    {
        PropertyConfigurator.configure(TestZKSample.class.getClassLoader().getResource("conf/log4j.properties").getPath());
        TestZKSample ts = new TestZKSample();
        try
        {
            ts.testSample();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
