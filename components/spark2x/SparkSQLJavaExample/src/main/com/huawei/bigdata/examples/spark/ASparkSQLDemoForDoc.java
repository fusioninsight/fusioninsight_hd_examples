package main.com.huawei.bigdata.examples.spark;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import com.huawei.bigdata.security.kerberos.LoginUtil;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.Serializable;
import java.util.List;

/*
 *   目标：
 *       统计星期六和星期天两天，上网总时长超过120分钟的女性
 *
 *   步骤：
 *       1. 加载原数据，生成RDD
 *       2. 将RDD装换为DataSet
 *       3. 创建临时表
 *       4. 通过SQL语句查询满足条件的记录
 */

public class ASparkSQLDemoForDoc {
    static {
        //日志配置文件
        PropertyConfigurator.configure(ASparkSQLDemoForDoc.class.getClassLoader().getResource("log4j.properties").getPath());
    }
    private final static Log LOG = LogFactory.getLog(ASparkSQLDemoForDoc.class.getName());

    static Configuration conf = new Configuration();
    static FileSystem fSystem;

    //将测试数据上传到HDFS
    static String SRC_DATA_PATH = ASparkSQLDemoForDoc.class.getClassLoader().getResource("").getPath() + File.separator + "data";
    static String DST_DATA_PATH = "/tmp/sparksql/testuser/testdata/";

    public static class FemaleInfo implements Serializable
    {
        private String name;
        private String gender;
        private Integer stayTime;

        public String getName() {
            return name;
        }
        public void setName(String name) {
            this.name = name;
        }
        public String getGender() {
            return gender;
        }
        public void setGender(String gender) {
            this.gender = gender;
        }
        public Integer getStayTime() {
            return stayTime;
        }
        public void setStayTime(Integer stayTime) {
            this.stayTime = stayTime;
        }
    }

    private static void prepareTestData() throws Exception
    {
        //HDFS代码详解介绍，请参考HDFS样例代码
        conf.addResource(new Path(ASparkSQLDemoForDoc.class.getClassLoader().getResource("hdfs-site.xml").getPath()));
        conf.addResource(new Path(ASparkSQLDemoForDoc.class.getClassLoader().getResource("core-site.xml").getPath()));

        fSystem = FileSystem.get(conf);

        Path srcDataPath = new Path(SRC_DATA_PATH);
        Path dstDataPath = new Path(DST_DATA_PATH);

        if (fSystem.exists(dstDataPath))
        {
            fSystem.delete(dstDataPath, true);
        }

        fSystem.copyFromLocalFile(srcDataPath, dstDataPath);
        FileStatus[] files = fSystem.listStatus(dstDataPath);
    }

    public static void main(String[] args) throws Exception
    {
        //*******认证 Start**************
        //配置认证配置文件和凭据，仅供测试使用。非安全版本和正式代码删除。正式提交任务推荐使用spark-submit方式。
        String PRNCIPAL_NAME = "zlt";//需要修改为实际在manager添加的用户
        String krb5Conf =  ASparkSQLDemoForDoc.class.getClassLoader().getResource("krb5.conf").getPath();
        String keyTab = ASparkSQLDemoForDoc.class.getClassLoader().getResource("user.keytab").getPath();
        LoginUtil.login(PRNCIPAL_NAME, keyTab, krb5Conf, conf);
        //*******认证 End**************

        //将样例数据上传到HDFS
        prepareTestData();

        //创建SparkSession。SparkSession为Spark的各项功能提供了统一的入口点，封装了SparkConf、SparkContext和SQLContext。
        SparkSession sparkSession = SparkSession.builder().appName("TestSparkSql").config("spark.master", "local[2]").getOrCreate();
        //从HDFS上读取样例数据，转换成RDD。
        JavaRDD<FemaleInfo> femaleInfoJavaRDD = sparkSession.read().textFile(DST_DATA_PATH).javaRDD().map(new Function<String, FemaleInfo>() {
            @Override
            public FemaleInfo call(String line) throws Exception
            {
                FemaleInfo femaleInfo = new FemaleInfo();
                String[] infoParts = line.split(",");
                femaleInfo.setName(infoParts[0]);
                femaleInfo.setGender(infoParts[1]);
                femaleInfo.setStayTime(Integer.parseInt(infoParts[2].trim()));
                return femaleInfo;
            }
        });

        //将RDD转化为DataSet对象，以便后续的SQL操作。使用Java的反射机制，由class的field推断schema。class必须是可序列化的。
        Dataset<Row> schemaFemaleInfo = sparkSession.createDataFrame(femaleInfoJavaRDD, FemaleInfo.class);
        //指定生成的临时表的名称
        schemaFemaleInfo.registerTempTable("FemaleInfoTable");
        //使用SQL语句操作DataSet，查找在网时间大于120分钟的女性
        Dataset<Row> femaleStayTime = sparkSession.sql("select * from " +
                "(select name,sum(stayTime) as totalStayTime from FemaleInfoTable " +
                "where gender = 'female' group by name )" +
                " tmp where totalStayTime >120");

        //汇总统计结果
        List<String> result = femaleStayTime.javaRDD().map(new Function<Row, String>()
        {
            @Override
            public String call(Row row)
            {
                return row.getString(0) + "," + row.getLong(1);
            }
        }).collect();

        LOG.info("########## Collect Result Start ############");
        LOG.info(result);
        LOG.info("########## Collect Result End ##############");
        sparkSession.stop();

        //删除测试数据，清理测试环境
        fSystem.delete(new Path(DST_DATA_PATH), true);
    }
}
