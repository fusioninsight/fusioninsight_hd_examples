package com.huawei.bigdata.hbase.examples;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
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

public class TestZKSample
{

    private static final Log LOG = LogFactory.getLog(TestZKSample.class.getName());

    private TableName tableName = null;

    private Configuration conf = null;

    private Connection conn = null;

    public void testSample() throws Exception
    {

        String keytabFile = "user.keytab";
        String principal = "hbaseuser1";
        tableName = TableName.valueOf("hbase_zk_sample_table");

        conf = login(keytabFile, principal);
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
            org.apache.zookeeper.ZooKeeper digestZk = null;
            System.setProperty("zookeeper.sasl.client", "true");
            System.setProperty("java.security.auth.login.config", "conf/jaas.conf");
            digestZk = new org.apache.zookeeper.ZooKeeper("127.0.0.1:2181", 60000, null);

            LOG.info("digest directory：\n" + digestZk.getChildren("/", null));

            LOG.info("Successfully connect to apache zookeeper.");
        }
        catch (InterruptedException e)
        {
            LOG.error("Found error when connect apache zookeeper ", e);
        }

    }

    public Configuration login(String keytabFile, String principal) throws Exception
    {
        Configuration conf = HBaseConfiguration.create();
        if (User.isHBaseSecurityEnabled(conf))
        {
            String confDirPath = System.getProperty("user.dir") + File.separator + "conf" + File.separator;

            // jaas.conf file, it is included in the client pakcage file
            System.setProperty("java.security.auth.login.config", confDirPath + "jaas.conf");

            // set the kerberos server info,point to the kerberosclient
            System.setProperty("java.security.krb5.conf", confDirPath + "krb5.conf");
            // set the keytab file name
            conf.set("username.client.keytab.file", confDirPath + keytabFile);
            // set the user's principal
            try
            {
                conf.set("username.client.kerberos.principal", principal);
                User.login(conf, "username.client.keytab.file", "username.client.kerberos.principal",
                        InetAddress.getLocalHost().getCanonicalHostName());
            }
            catch (IOException e)
            {
                throw new Exception("Login failed.");
            }
        }
        return conf;
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
