package com.huawei.bigdata.gaussDB;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class GaussDBSolution extends Thread {
    private Statement cancel_stmt = null;

    public GaussDBSolution(Statement stmt) {
        super("Exit Handler");
        this.cancel_stmt = stmt;
    }
    public void run() {
        System.out.println("exit handle");
        try {
            this.cancel_stmt.cancel();
        } catch (SQLException e) {
            System.out.println("cancel queyr failed.");
            e.printStackTrace();
        }
    }
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
    }
//    //执行普通SQL语句，创建jdbc_test1表。
//    public static void CreateTable(Connection conn) {
//        Statement stmt = null;
//        try {
//            stmt = conn.createStatement();
//
//            // add ctrl+c handler
//            Runtime.getRuntime().addShutdownHook(new ExitHandler(stmt));
//
//            //执行普通SQL语句。
//            int rc = stmt
//                    .executeUpdate("CREATE TABLE jdbc_test1(col1 INTEGER, col2 VARCHAR(10));");
//
//            stmt.close();
//        } catch (SQLException e) {
//            if (stmt != null) {
//                try {
//                    stmt.close();
//                } catch (SQLException e1) {
//                    e1.printStackTrace();
//                }
//            }
//            e.printStackTrace();
//        }
//    }
//
//    //执行预处理语句，批量插入数据。
//    public static void BatchInsertData(Connection conn) {
//        PreparedStatement pst = null;
//
//        try {
//            //生成预处理语句。
//            pst = conn.prepareStatement("INSERT INTO jdbc_test1 VALUES (?,?)");
//            for (int i = 0; i < 3; i++) {
//                //添加参数。
//                pst.setInt(1, i);
//                pst.setString(2, "data " + i);
//                pst.addBatch();
//            }
//            //执行批处理。
//            pst.executeBatch();
//            pst.close();
//        } catch (SQLException e) {
//            if (pst != null) {
//                try {
//                    pst.close();
//                } catch (SQLException e1) {
//                    e1.printStackTrace();
//                }
//            }
//            e.printStackTrace();
//        }
//    }
//
//    //执行预编译语句，更新数据。
//    public static void ExecPreparedSQL(Connection conn) {
//        PreparedStatement pstmt = null;
//        try {
//            pstmt = conn
//                    .prepareStatement("UPDATE jdbc_test1 SET col2 = ? WHERE col1 = 1");
//            pstmt.setString(1, "new Data");
//            int rowcount = pstmt.executeUpdate();
//            pstmt.close();
//        } catch (SQLException e) {
//            if (pstmt != null) {
//                try {
//                    pstmt.close();
//                } catch (SQLException e1) {
//                    e1.printStackTrace();
//                }
//            }
//            e.printStackTrace();
//        }
//    }
//
//
//    //执行存储过程。
//    public static void ExecCallableSQL(Connection conn) {
//        CallableStatement cstmt = null;
//        try {
//
//            cstmt=conn.prepareCall("{? = CALL TESTPROC(?,?,?)}");
//            cstmt.setInt(2, 50);
//            cstmt.setInt(1, 20);
//            cstmt.setInt(3, 90);
//            cstmt.registerOutParameter(4, Types.INTEGER);  //注册out类型的参数，类型为整型。
//            cstmt.execute();
//            int out = cstmt.getInt(4);  //获取out参数
//            System.out.println("The CallableStatment TESTPROC returns:"+out);
//            cstmt.close();
//        } catch (SQLException e) {
//            if (cstmt != null) {
//                try {
//                    cstmt.close();
//                } catch (SQLException e1) {
//                    e1.printStackTrace();
//                }
//            }
//            e.printStackTrace();
//        }
//    }
//
//
//    /**
//     * 主程序，逐步调用各静态方法。
//     * @param args
//     */
    public static void main(String[] args) {
        //创建数据库连接。
        Connection conn = GetConnection("tester", "Password1234");

        //创建表。
//        CreateTable(conn);
//
//        //批插数据。
//        BatchInsertData(conn);
//
//        //执行预编译语句，更新数据。
//        ExecPreparedSQL(conn);
//
//        //执行存储过程。
//        ExecCallableSQL(conn);

        //关闭数据库连接。
        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

}


