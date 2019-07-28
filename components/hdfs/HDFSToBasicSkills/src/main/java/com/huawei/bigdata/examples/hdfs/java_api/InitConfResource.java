package com.huawei.bigdata.examples.hdfs.java_api;

import java.util.Properties;

public class InitConfResource {
    private  static Properties properties = new Properties();
    static {
        try{
            properties.load(InitConfResource.class.getClassLoader().getResourceAsStream("conf/ConfFile.properties"));
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    public final static String userName = properties.getProperty("User.Name");
    public final static String userkeytab = properties.getProperty("User.keytab");
    public final static String kerberos = properties.getProperty("Krb5.conf");
    public final static String mkdirPath = properties.getProperty("Dir.path");
    public final static String mkdirTxt = properties.getProperty("mkdir.Txt");
    public final static String hdfsPath = properties.getProperty("Hdfs.Path");
    public final static String coreSite = properties.getProperty("Core.site");

}
