package com.huawei.bigdata.hdfs.examples;

import java.io.*;
import java.net.URI;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import com.huawei.hadoop.oi.colocation.DFSColocationAdmin;
import com.huawei.hadoop.oi.colocation.DFSColocationClient;
import com.huawei.hadoop.security.LoginUtil;

public class ColocationExample
{

    private static final String TESTFILE_TXT = "/testfile.txt";

    private static final String COLOCATION_GROUP_GROUP01 = "gid01";

    private static final String ZOOKEEPER_SERVER_PRINCIPAL_KEY = "zookeeper.server.principal";

    private static final String ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL = "zookeeper/hadoop.hadoop.com";

    private static Configuration conf = new Configuration();

    private static DFSColocationAdmin dfsAdmin;

    private static DFSColocationClient dfs;

    private static String PRINCIPAL = "username.client.kerberos.principal";

    private static String KEYTAB = "username.client.keytab.file";

    // This is the principal name of keytab.
    private static String PRNCIPAL_NAME = "test";

    private static String LOGIN_CONTEXT_NAME = "Client";

    private static String PATH_TO_KEYTAB = System.getProperty("user.dir") + File.separator + "conf" + File.separator
            + "user.keytab";

    private static String PATH_TO_KRB5_CONF = ColocationExample.class.getClassLoader().getResource("krb5.conf")
            .getPath();

    private static void init() throws IOException
    {
        conf.set(KEYTAB, PATH_TO_KEYTAB);
        conf.set(PRINCIPAL, PRNCIPAL_NAME);

        LoginUtil.setJaasConf(LOGIN_CONTEXT_NAME, PRNCIPAL_NAME, PATH_TO_KEYTAB);
        LoginUtil.setZookeeperServerPrincipal(ZOOKEEPER_SERVER_PRINCIPAL_KEY, ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL);
        LoginUtil.login(PRNCIPAL_NAME, PATH_TO_KEYTAB, PATH_TO_KRB5_CONF, conf);
    }

    private static void put() throws IOException
    {
        FSDataOutputStream out = dfs.create(new Path(TESTFILE_TXT), true, COLOCATION_GROUP_GROUP01, "lid01");
        // the data to be written to the hdfs.
        try
        {
            byte[] readBuf = "Hello World".getBytes("UTF-8");
            out.write(readBuf, 0, readBuf.length);
            out.close();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        finally
        {
            out.close();
        }
    }

    private static void delete() throws IOException
    {
        dfs.delete(new Path(TESTFILE_TXT));
    }

    private static void createGroup() throws IOException
    {
        dfsAdmin.createColocationGroup(COLOCATION_GROUP_GROUP01,
                Arrays.asList(new String[] { "lid01", "lid02", "lid03" }));
    }

    private static void deleteGroup() throws IOException
    {
        dfsAdmin.deleteColocationGroup(COLOCATION_GROUP_GROUP01);
    }

    public static void main(String[] args) throws IOException
    {
        init();
        dfsAdmin = new DFSColocationAdmin(conf);
        dfs = new DFSColocationClient();
        dfs.initialize(URI.create("hdfs://hacluster"), conf);

        System.out.println("Create Group is running...");
        createGroup();
        System.out.println("Create Group has finished.");

        System.out.println("Put file is running...");
        put();
        System.out.println("Put file has finished.");

        System.out.println("Delete file is running...");
        delete();
        System.out.println("Delete file has finished.");

        System.out.println("Delete Group is running...");
        deleteGroup();
        System.out.println("Delete Group has finished.");

        dfs.close();
        dfsAdmin.close();
    }

}
