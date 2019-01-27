package com.huawei.bigdata.Elk.example;


import com.huawei.bigdata.Elk.LoginUtil.LoginUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.PropertyConfigurator;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class HdfsToElk {
    static {
        //日志配置文件
        PropertyConfigurator.configure(HdfsToElk.class.getClassLoader().getResource("log4j.properties").getPath());
    }
    private final static Log LOG = LogFactory.getLog(HdfsToElk.class.getName());
    public static void main(String[] args)throws Exception {
        String FILE_PATH = "/zwl/test.txt";
        String userName = "joe";
        String pw = "Bigdata@123";
        Configuration conf = getLoginUtil();
        Connection conn = getConnection(userName, pw);
        String sqlText1 = "CREATE TABLE IF NOT EXISTS tradeInfo(bankCount VARCHAR(128),name VARCHAR(128)," +
                "IDCard VARCHAR(128),tradeDate VARCHAR(128),tradeAmount INTEGER,tradeType VARCHAR(32)," +
                "desBankCount VARCHAR(128),desName VARCHAR(128),desIDCard VARCHAR(128),remark VARCHAR(128)) tablespace hdfs;";
        String sqlText2 = "CREATE TABLE IF NOT EXISTS accountInfo(bankCount VARCHAR(128),name VARCHAR(128),IDCard VARCHAR(128)) tablespace hdfs;";
        createTable(conn, sqlText1);
        createTable(conn, sqlText2);
        FileSystem file = FileSystem.get(conf);
        Path dirpath = new Path(FILE_PATH);
        System.out.println(FILE_PATH);
        System.out.println(dirpath);
        if (file.exists(dirpath)) {
            FSDataInputStream in = null;
            BufferedReader reader = null;
            StringBuilder stringBuilder = new StringBuilder();
            List<String> list = new ArrayList<String>();
            in = file.open(dirpath);
            reader = new BufferedReader(new InputStreamReader(in));
            String sTempOneLine;
            while ((sTempOneLine = reader.readLine()) != null) {
                list.add(sTempOneLine);
            }
            PreparedStatement pst = null;
            try {
                conn = getConnection(userName, pw);
                pst = conn.prepareStatement("INSERT INTO tradeInfo(bankCount,name,IDCard,tradeDate,tradeAmount" +
                        ",tradeType,desBankCount,desName,desIDCard,remark) VALUES (?,?,?,?,?,?,?,?,?,?)");
                for (int i = 0; i < list.size(); i++) {
                    String[] data = list.get(i).split("\\|");
                    pst.setString(1, data[0]);
                    pst.setString(2, data[1]);
                    pst.setString(3, data[2]);
                    pst.setString(4, data[3]);
                    pst.setString(5, data[4]);
                    pst.setString(6, data[5]);
                    pst.setString(7, data[6]);
                    pst.setString(8, data[7]);
                    pst.setString(9, data[8]);
                    pst.setString(10, data[9]);
                    pst.addBatch();
                }
                pst.executeBatch();
                pst.close();
            } catch (SQLException e) {
                if (pst != null) {
                    try {
                        pst.close();
                    } catch (SQLException e1) {
                        e1.printStackTrace();
                    }
                }
                e.printStackTrace();
            }

            try {
                conn = getConnection(userName, pw);
                pst = conn.prepareStatement("INSERT INTO accountinfo(bankCount,name,IDCard) VALUES (?,?,?)");
                for (int i = 0; i < list.size(); i++) {
                    String[] data = list.get(i).split("\\|");
                    pst.setString(1, data[0]);
                    pst.setString(2, data[1]);
                    pst.setString(3, data[2]);
                    pst.addBatch();
                }
                pst.executeBatch();
                pst.close();
            } catch (SQLException e) {
                if (pst != null) {
                    try {
                        pst.close();
                    } catch (SQLException e1) {
                        e1.printStackTrace();
                    }
                }
                e.printStackTrace();
            }
        }
    }
    public static Connection getConnection(String userName,String pw)
    {
        System.out.println("Begin get Elk's connection");
        String driver = "org.postgresql.Driver";
        String sourceURL = "jdbc:postgresql://187.5.88.121:25108/postgres";
        Connection conn = null;
        try{
            Class.forName(driver);
        }catch (Exception e){
            e.printStackTrace();
        }
        try {
            conn = DriverManager.getConnection(sourceURL,userName, pw);
            System.out.println("get Elk's connection succeed");
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }
        return conn;
    }
    public static void createTable(Connection conn,String sqlText){
        Statement stmt = null;
        try {
            stmt=conn.createStatement();
            int rc = stmt.executeUpdate(sqlText);
            stmt.close();
        }catch (SQLException e){
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e1) {
                    e1.printStackTrace();
                }
            }
            e.printStackTrace();
        }
    }
public static Configuration getLoginUtil() throws Exception{
    Configuration conf = new Configuration();
    conf.addResource(new Path(HdfsToElk.class.getClassLoader().getResource("hdfs-site.xml").getPath()));
    conf.addResource(new Path(HdfsToElk.class.getClassLoader().getResource("core-site.xml").getPath()));
    if("kerberos".equalsIgnoreCase(conf.get("hadoop.security.authentication")))
    {
        String PRNCIPAL_NAME = "lyysxg";//需要修改为实际在manager添加的用户
        String KRB5_CONF = HdfsToElk.class.getClassLoader().getResource("krb5.conf").getPath();
        String KEY_TAB = HdfsToElk.class.getClassLoader().getResource("user.keytab").getPath();

        System.setProperty("java.security.krb5.conf", KRB5_CONF); //指定kerberos配置文件到JVM
        LoginUtil.login(PRNCIPAL_NAME, KEY_TAB, KRB5_CONF, conf);

    }
    return  conf;
}

}
