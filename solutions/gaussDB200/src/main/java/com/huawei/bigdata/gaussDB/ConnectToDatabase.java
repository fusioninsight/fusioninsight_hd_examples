package com.huawei.bigdata.gaussDB;

import java.util.Properties;

public  class ConnectToDatabase {
    public static Connection GetConnection(String username, String passwd)
{
    //驱动类。
    String driver = "org.postgresql.Driver";
    //设置keystore
    System.setProperty("javax.net.ssl.trustStore", "mytruststore");
    System.setProperty("javax.net.ssl.keyStore", "client.jks");
    System.setProperty("javax.net.ssl.trustStorePassword", "password");
    System.setProperty("javax.net.ssl.keyStorePassword", "password");

    Properties props = new Properties();
    props.setProperty("user", "CLIENT");
    props.setProperty("password", "1234@qwer");
    props.setProperty("ssl", "true");

    String  url = "jdbc:postgresql://" + "10.144.197.193" + ':'
            + "8511" + '/'
            + "POSTGRES";
    Connection conn = null;

    try
    {
        //加载驱动。
        Class.forName(driver);
    }
    catch( Exception e )
    {
        e.printStackTrace();
        return null;
    }

    try
    {
        //创建连接。
        conn = DriverManager.getConnection(url, props );
        System.out.println("Connection succeed!");
    }
    catch(Exception e)
    {
        e.printStackTrace();
        return null;
    }

    return conn;
};

}
