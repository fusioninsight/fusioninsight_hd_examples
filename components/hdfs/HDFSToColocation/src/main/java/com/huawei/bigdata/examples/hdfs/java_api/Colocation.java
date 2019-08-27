package com.huawei.bigdata.examples.hdfs.java_api;

import java.io.File;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;

import com.huawei.bigdata.examples.hdfs.security.LoginUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import com.huawei.hadoop.oi.colocation.DFSColocationAdmin;
import com.huawei.hadoop.oi.colocation.DFSColocationClient;

/**
   * Colocation样例代码
   * 同分布（Colocation）功能是将存在关联关系的数据或可能要进行关联操作的数据存储在相同的存储节点上。
   * 在进行关联操作计算时避免了到别的数据节点上获取数据，大大降低网络带宽的占用。
   * （功能详细介绍请参考产品文档）
   */

public class Colocation
{
    private static final String PRNCIPAL_NAME = "lyysxg";
  
    private static final String TESTFILE_TXT = "/testfile.txt";

    private static final String COLOCATION_GROUP_GROUP01 = "gid01";

    private static final String ZOOKEEPER_SERVER_PRINCIPAL_KEY = "zookeeper.server.principal";

    private static final String ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL = "zookeeper/hadoop";

    private static Configuration conf = new Configuration();

    private static DFSColocationAdmin dfsAdmin;

    private static DFSColocationClient dfs;

    private static String PRINCIPAL = "username.client.kerberos.principal";

    private static String KEYTAB = "username.client.keytab.file";

    private static String LOGIN_CONTEXT_NAME = "Client";

    private static String PATH_TO_KEYTAB = Colocation.class.getClassLoader().getResource("conf/user.keytab")
            .getPath();

    private static String PATH_TO_KRB5_CONF = Colocation.class.getClassLoader().getResource("conf/krb5.conf")
            .getPath();

    private static void init() throws IOException
    {
        conf.set(KEYTAB, PATH_TO_KEYTAB);
        conf.set(PRINCIPAL, PRNCIPAL_NAME);

        LoginUtil.setJaasConf(LOGIN_CONTEXT_NAME, PRNCIPAL_NAME, PATH_TO_KEYTAB);
        LoginUtil.setZookeeperServerPrincipal(ZOOKEEPER_SERVER_PRINCIPAL_KEY, ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL);
        LoginUtil.login(PRNCIPAL_NAME, PATH_TO_KEYTAB, PATH_TO_KRB5_CONF, conf);
    }

    /**
     * create and write file
     * 
     * @throws java.io.IOException
     */
    private static void put() throws IOException
    {
        FSDataOutputStream out = dfs.create(new Path(TESTFILE_TXT), true, COLOCATION_GROUP_GROUP01, "lid01");
        // the data to be written to the hdfs.
        byte[] readBuf = "Hello World".getBytes("UTF-8");
        out.write(readBuf, 0, readBuf.length);
        out.close();
    }

    /**
     * delete file
     * 
     * @throws java.io.IOException
     */
    @SuppressWarnings("deprecation")
    private static void delete() throws IOException
    {
        dfs.delete(new Path(TESTFILE_TXT));
    }

    /**
     * create group
     * 
     * @throws java.io.IOException
     */
    private static void createGroup() throws IOException
    {
        dfsAdmin.createColocationGroup(COLOCATION_GROUP_GROUP01,
                Arrays.asList(new String[] { "lid01", "lid02", "lid03" }));
    }

    /**
     * delete group
     * 
     * @throws java.io.IOException
     */
    private static void deleteGroup() throws IOException
    {
        dfsAdmin.deleteColocationGroup(COLOCATION_GROUP_GROUP01);
    }

    public static void main(String[] args) throws IOException
    {
        init();
        dfsAdmin = new DFSColocationAdmin(conf);
        dfs = new DFSColocationClient();
        dfs.initialize(URI.create(conf.get("fs.defaultFS")), conf);

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
