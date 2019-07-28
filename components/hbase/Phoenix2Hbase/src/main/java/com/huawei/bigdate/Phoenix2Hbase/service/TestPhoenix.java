package com.huawei.bigdate.Phoenix2Hbase.service;

import com.huawei.bigdate.Phoenix2Hbase.model.Vehicle;
import com.huawei.bigdate.Phoenix2Hbase.security.LoginUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Service
public class TestPhoenix {
    private final static Log LOG = LogFactory.getLog(TestPhoenix.class.getName());

    private static final String ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME = "Client";
    private static final String ZOOKEEPER_SERVER_PRINCIPAL_KEY = "zookeeper.server.principal";
    private static final String ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL = "zookeeper/hadoop.hadoop.com";

    private static Configuration conf = null;
    private static String krb5File = null;
    private static String userName = null;
    private static String userKeytabFile = null;

    public static void initAndLogin() {
        try {
            init();
            login();
        } catch (IOException e) {
            LOG.error("Failed to login because ", e);
            return;
        }
    }

    private static void login() throws IOException {
            String userdir = System.getProperty("user.dir")+ File.separator+"src" +File.separator+"main" + File.separator+"resources"   +File.separator;
            //使用机机用户名，对应resources下的user.keytab、krb5.conf用于安全认证
            userName = "fanC80_jj";
            userKeytabFile = userdir + "user.keytab";
            krb5File = userdir + "krb5.conf";

            /*
             * if need to connect zk, please provide jaas info about zk. of course,
             * you can do it as below:
             * System.setProperty("java.security.auth.login.config", confDirPath +
             * "jaas.conf"); but the demo can help you more : Note: if this process
             * will connect more than one zk cluster, the demo may be not proper. you
             * can contact us for more help
             */
            LoginUtil.setJaasConf(ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME, userName, userKeytabFile);
            LoginUtil.setZookeeperServerPrincipal(ZOOKEEPER_SERVER_PRINCIPAL_KEY,
                    ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL);
            LoginUtil.login(userName, userKeytabFile, krb5File, conf);
    }

    private static void init() throws IOException {
        // Default load from conf directory
        conf = HBaseConfiguration.create();

        String userdir = System.getProperty("user.dir")+ File.separator+"src" +File.separator+"main"  + File.separator+"resources"  +File.separator;

        LOG.info("=======userdir:"+userdir);
        conf.addResource(new Path(userdir + "core-site.xml"));
        conf.addResource(new Path(userdir + "hdfs-site.xml"));
        conf.addResource(new Path(userdir + "hbase-site.xml"));

    }
//
//    public static void  testPhoenixSample(){
//        PhoenixSample oneSample = null;
//        try {
//            oneSample = new PhoenixSample(conf);
//            oneSample.testCreateTable();
//            oneSample.testPut();
//            oneSample.testSelect();
//            oneSample.testMySelect();
//            oneSample.testDrop();
//        } catch (Exception e) {
//            LOG.error("Failed to test HBase because ", e);
//        }
//        LOG.info("-----------finish HBase -------------------");
//    }
    public  void  testCreateTable(String tableName){
        PhoenixSample oneSample = null;
        try {
            oneSample = new PhoenixSample(conf,tableName);
            oneSample.testCreateTable();
        } catch (Exception e) {
            LOG.error("Failed to test HBase because ", e);
        }
        LOG.info("-----------finish testCreateTable -------------------");
    }

    public  void  testPut(Vehicle vehicle,String tableName){
        PhoenixSample oneSample = null;
        try {
            oneSample = new PhoenixSample(conf,tableName);
            oneSample.testPut(vehicle);
        } catch (Exception e) {
            LOG.error("Failed to test HBase because ", e);
        }
        LOG.info("-----------finish testPut -------------------");
    }

    public  Vehicle  testSelectFromId(String id,String tableName){
        Vehicle vehicle = new Vehicle();
        PhoenixSample oneSample = null;
        try {
            oneSample = new PhoenixSample(conf,tableName);
            vehicle =  oneSample.testSelectFromId(id);
        } catch (Exception e) {
            LOG.error("Failed to test HBase because ", e);
        }
        LOG.info("-----------finish testSelectFromId -------------------");
        return vehicle;
    }

    public  Vehicle  testSelectFromPlateNo(String plateNo,String tableName){
        Vehicle vehicle = new Vehicle();
        PhoenixSample oneSample = null;
        try {
            oneSample = new PhoenixSample(conf,tableName);
            vehicle =  oneSample.testSelectFromPlateNo(plateNo);
        } catch (Exception e) {
            LOG.error("Failed to test HBase because ", e);
        }
        LOG.info("-----------finish testSelectFromPlateNo -------------------");
        return vehicle;
    }


    public  List<Vehicle>  testSelectFixedNumData(int num ,String tableName){
        PhoenixSample oneSample = null;
        List<Vehicle>  results = new ArrayList<Vehicle>();
        try {
            oneSample = new PhoenixSample(conf,tableName);
            results = oneSample.testSelectFixedNumData(num);
        } catch (Exception e) {
            LOG.error("Failed to test HBase because ", e);
        }
        LOG.info("-----------finish testSelectFixedNumData -------------------");
        return results;
    }


    public  List<Vehicle>  testSelectFromTime(String time ,String tableName){
        PhoenixSample oneSample = null;
        List<Vehicle>  results = new ArrayList<Vehicle>();
        try {
            oneSample = new PhoenixSample(conf,tableName);
            results = oneSample.testSelectFromTime(time);
        } catch (Exception e) {
            LOG.error("Failed to test HBase because ", e);
        }
        LOG.info("-----------finish testSelectFromTime -------------------");
        return results;
    }

    public  List<Vehicle>  testSelectFromVehicleColorAndType(int color,String type ,String tableName){
        List<Vehicle>  results = new ArrayList<Vehicle>();
        PhoenixSample oneSample = null;
        try {
            oneSample = new PhoenixSample(conf,tableName);
            results = oneSample.testSelectFromVehicleColorAndType(color,type);
        } catch (Exception e) {
            LOG.error("Failed to test HBase because ", e);
        }
        LOG.info("-----------finish testSelectFromVehicleColorAndType -------------------");
        return results;
    }

    public  List<Vehicle>  testSelectFromSafebelt(int sfebelt ,String tableName){
        List<Vehicle>  results = new ArrayList<Vehicle>();
        PhoenixSample oneSample = null;
        try {
            oneSample = new PhoenixSample(conf,tableName);
            results = oneSample.testSelectFromSafebelt(sfebelt);
        } catch (Exception e) {
            LOG.error("Failed to test HBase because ", e);
        }
        LOG.info("-----------finish testSelectFromSafebelt -------------------");
        return results;
    }


    public  void  testDrop(String tableName){
        PhoenixSample oneSample = null;
        try {
            oneSample = new PhoenixSample(conf,tableName);
            oneSample.testDrop();
        } catch (Exception e) {
            LOG.error("Failed to test HBase because ", e);
        }
        LOG.info("-----------finish testDrop -------------------");
    }

    public  void  testDelete(String id,String tableName){
        PhoenixSample oneSample = null;
        try {
            oneSample = new PhoenixSample(conf,tableName);
            oneSample.testDelete(id);
        } catch (Exception e) {
            LOG.error("Failed to test HBase because ", e);
        }
        LOG.info("-----------finish testDelete -------------------");
    }
}
