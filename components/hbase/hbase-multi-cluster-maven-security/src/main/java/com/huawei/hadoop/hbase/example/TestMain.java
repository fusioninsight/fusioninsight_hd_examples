package com.huawei.hadoop.hbase.example;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.huawei.bigdata.hdfs.examples.HdfsExample;
import com.huawei.hadoop.security.LoginUtil;

public class TestMain
{
    private static Logger LOG = Logger.getLogger(LoginUtil.class);

    private static final String ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME = "Client";

    private static final String ZOOKEEPER_SERVER_PRINCIPAL_KEY = "zookeeper.server.principal";

    private static final String ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL = "zookeeper/hadoop";

    private static Configuration CONF_CLUSTER1 = null;

    private static Configuration CONF_CLUSTER2 = null;

    private static String KRB5_FILE = null;

    private static String USER_NAME = null;

    private static String USER_KEYTAB_FILE = null;

    public static void main(String[] args) throws Exception
    {
        PropertyConfigurator.configure(TestMain.class.getClassLoader().getResource("conf/log4j.properties").getPath());
        init();
        LoginUtil.login(USER_NAME, USER_KEYTAB_FILE, KRB5_FILE, CONF_CLUSTER1);

        // test HBase
        HBaseSample ts = new HBaseSample(CONF_CLUSTER1);
        ts.testCreateTable();

        // test HDFS
        HdfsExample hdfs_examples = new HdfsExample(CONF_CLUSTER2);
        hdfs_examples.mkdir();
        hdfs_examples.write();
        hdfs_examples.append();
        hdfs_examples.read();
        hdfs_examples.delete();

        ts.dropTable();

    }

    public static void init() throws Exception
    {
        CONF_CLUSTER1 = new Configuration();

        CONF_CLUSTER1
                .addResource(new Path(TestMain.class.getClassLoader().getResource("conf/core-site.xml").getPath()));
        CONF_CLUSTER1
                .addResource(new Path(TestMain.class.getClassLoader().getResource("conf/hdfs-site.xml").getPath()));
        CONF_CLUSTER1
                .addResource(new Path(TestMain.class.getClassLoader().getResource("conf/hbase-site.xml").getPath()));
        CONF_CLUSTER1
                .addResource(new Path(TestMain.class.getClassLoader().getResource("conf/hdfs-site.xml").getPath()));

        CONF_CLUSTER2 = new Configuration();
        CONF_CLUSTER2
                .addResource(new Path(TestMain.class.getClassLoader().getResource("conf2/core-site.xml").getPath()));
        CONF_CLUSTER2
                .addResource(new Path(TestMain.class.getClassLoader().getResource("conf2/hdfs-site.xml").getPath()));

        USER_NAME = "test001";
        USER_KEYTAB_FILE = TestMain.class.getClassLoader().getResource("conf/user.keytab").getPath();
        KRB5_FILE = TestMain.class.getClassLoader().getResource("conf/krb5.conf").getPath();

        /*
         * if need to connect zk, please provide jaas info about zk. of course, you can do it as below:
         * System.setProperty("java.security.auth.login.config", confDirPath + "jaas.conf"); but the demo can help you
         * more : Note: if this process will connect more than one zk cluster, the demo may be not proper. you can
         * contact us for more help
         */
        LoginUtil.setJaasFile(USER_NAME, USER_KEYTAB_FILE);
        LoginUtil.setZookeeperServerPrincipal(ZOOKEEPER_SERVER_PRINCIPAL_KEY, ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL);
    }

}
