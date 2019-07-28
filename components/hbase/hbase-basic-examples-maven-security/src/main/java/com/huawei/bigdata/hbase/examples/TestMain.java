package com.huawei.bigdata.hbase.examples;

import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.security.User;
import org.apache.log4j.PropertyConfigurator;

import com.huawei.bigdata.security.LoginUtil;
import java.net.URL;

public class TestMain
{
    private final static Log LOG = LogFactory.getLog(TestMain.class.getName());

    private static final String ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME = "Client";

    private static final String ZOOKEEPER_SERVER_PRINCIPAL_KEY = "zookeeper.server.principal";

    private static final String ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL = "zookeeper/hadoop.hadoop.com";

    private static Configuration conf = null;

    public static void main(String[] args)
    {
      PropertyConfigurator.configure(TestMain.class.getClassLoader().getResource("conf/log4j.properties").getPath());
        try
        {
            init();
            login();
        }
        catch (IOException e)
        {
            LOG.error("Failed to login because ", e);
            return;
        }
        // getDefaultConfiguration();

        // test hbase
        HBaseSample oneSample;
        try
        {
            oneSample = new HBaseSample(conf);
            oneSample.test();
        }
        catch (Exception e)
        {
            LOG.error("Failed to test HBase because ", e);
        }
        LOG.info("-----------finish HBase -------------------");
    }

    private static void login() throws IOException
    {
        if (User.isHBaseSecurityEnabled(conf))
        {
            String userName = "test001";
            String userKeytabFile = TestMain.class.getClassLoader().getResource("conf/user.keytab").getPath();
            String krb5File = TestMain.class.getClassLoader().getResource("conf/krb5.conf").getPath();

            /*
             * if need to connect zk, please provide jaas info about zk. of course, you can do it as below:
             * System.setProperty("java.security.auth.login.config", confDirPath + "jaas.conf"); but the demo can help
             * you more : Note: if this process will connect more than one zk cluster, the demo may be not proper. you
             * can contact us for more help
             */
            LoginUtil.setJaasFile(userName,userKeytabFile);
            //LoginUtil.setJaasConf(ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME, userName, userKeytabFile);
            LoginUtil.setZookeeperServerPrincipal(ZOOKEEPER_SERVER_PRINCIPAL_KEY, ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL);
            LoginUtil.login(userName, userKeytabFile, krb5File, conf);
        }
    }

    private static void init() throws IOException
    {
        // Default load from conf directory
        conf = HBaseConfiguration.create();
        String userDir = TestMain.class.getClassLoader().getResource("conf").getPath() + File.separator;
         conf.addResource(new Path(userDir + "core-site.xml"), false);
         conf.addResource(new Path(userDir + "hdfs-site.xml"), false);
         conf.addResource(new Path(userDir + "hbase-site.xml"), false);
    }

}
