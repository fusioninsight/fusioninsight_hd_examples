package com.huawei.bigdata.Elk.example;

import com.huawei.bigdata.Elk.LoginUtil.LoginUtil;
import org.apache.hadoop.conf.Configuration;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

public class LaunderMoney {
    public static void main(String[] args)throws Exception{
        String userName ="joe";
        String pw = "Bigdata@123";
        HdfsToElk.getLoginUtil();
        Connection conn =HdfsToElk.getConnection(userName,pw);
        PreparedStatement pst = null;
        String sql1 = "select bankCount from tradeInfo group by bankCount";
        pst = conn.prepareStatement(sql1);
        ResultSet rs = pst.executeQuery();
        List<String> launderAcount = new ArrayList<String>();
        while(rs.next()){
            String account = rs.getString(1);
            String sql2 = "select desBankCount from tradeInfo where bankCount="+account+" group by desBankCount";
            pst = conn.prepareStatement(sql2);
            ResultSet rs2 = pst.executeQuery();
            while (rs2.next())
            {
                String account2 = rs2.getString(1);
                if(!account2.equalsIgnoreCase(account)){
                    String sql3 = "select desBankCount from tradeInfo where bankCount="+account2 +"group by desBankCount";
                    pst = conn.prepareStatement(sql3);
                    ResultSet rs3 = pst.executeQuery();
                    while (rs3.next()){
                        String account3 = rs3.getString(1);
                        if(account3.equalsIgnoreCase(account)){
                            launderAcount.add(account);
                        }else {
                            String sql4 = "select desBankCount from tradeInfo where bankCount="+account3 +"group by desBankCount";
                            pst = conn.prepareStatement(sql4);
                            ResultSet rs4 = pst.executeQuery();
                            while (rs4.next())
                            {
                                String account4 = rs4.getString(1);
                                if(account4.equalsIgnoreCase(account)){
                                    launderAcount.add(account);
                                }else {
                                    String sql5 = "select desBankCount from tradeInfo where bankCount="+account4+" group by desBankCount";
                                    pst = conn.prepareStatement(sql5);
                                    ResultSet rs5 = pst.executeQuery();
                                    while (rs5.next()){
                                        String acconut5 = rs5.getString(1);
                                        if(acconut5.equalsIgnoreCase(account)){
                                            launderAcount.add(acconut5);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

        }
    }
}
