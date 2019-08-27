package com.huawei.bigdata.hive.examples;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.Properties;

public class InitConfResource {
    private  static Properties properties = new Properties();
    private  static Properties properties1 = new Properties();
    static {
        try{
            properties.load(InitConfResource.class.getClassLoader().getResourceAsStream("conf/ConfFile.properties"));
            String hiveClient = properties.getProperty("HiveClient");
            properties1.load(new InputStreamReader(new FileInputStream(hiveClient)));
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    public final static String userName = properties.getProperty("User.Name");
    public final static String userkeytab = properties.getProperty("User.keytab");
    public final static String userkrb5 = properties.getProperty("Krb5.conf");
    public final static String hdfsPath = properties.getProperty("Hdfs.Path");
    public final static String coreSite = properties.getProperty("Core.site");
    public final static String log4j = properties.getProperty("log4j.path");
    public final static String auth = properties1.getProperty("auth");
    public final static String zkPort = properties1.getProperty("zk.port");
    public final static String zkQuorum = properties1.getProperty("zk.quorum");
    public final static String beelineEntirelineascommand = properties1.getProperty("beeline.entirelineascommand");
    public final static String principal = properties1.getProperty("principal");
    public final static String saslQop = properties1.getProperty("sasl.qop");
    public final static String zooKeeperNamespace = properties1.getProperty("zooKeeperNamespace");
    public final static String serviceDiscoveryMode = properties1.getProperty("serviceDiscoveryMode");
    public final static String instanceNo = properties1.getProperty("instanceNo");
    public static final String HIVE_DRIVER = properties.getProperty("HIVE.DRIVER");
    public static final String ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME = properties.getProperty("ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME");
    public static final String ZOOKEEPER_SERVER_PRINCIPAL_KEY = properties.getProperty("ZOOKEEPER_SERVER_PRINCIPAL_KEY");
    public static final String ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL = properties.getProperty("ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL");
}
