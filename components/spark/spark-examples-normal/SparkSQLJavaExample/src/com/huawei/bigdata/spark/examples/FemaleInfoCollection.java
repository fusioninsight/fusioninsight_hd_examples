package com.huawei.bigdata.spark.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import com.huawei.hadoop.security.LoginUtil;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

public class FemaleInfoCollection {
    public static class FemaleInfo implements Serializable {
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

    public static void main(String[] args) throws Exception {
    
        //启动Spark，所必须的步骤。SparkConfig包含了Spark集群的各种参数。setAppName就是在Wep端显示应用名而已。
        SparkConf conf = new SparkConf().setAppName("CollectFemaleInfo");

        //用于从系统属性加载设置。
        JavaSparkContext jsc = new JavaSparkContext(conf);

        //在Spark 1.x中处理结构化数据（行和列）的入口点。
        //从Spark 2.0开始，它被替换为SparkSession。但是，为了向后兼容，我们将此类保留在此处。
        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(jsc);

        //将文本转换为RDD输出
        JavaRDD<FemaleInfo> femaleInfoJavaRDD = jsc.textFile(args[0]).map(
                // Function函数，传入一个String参数，返回一个FemaleInfo对象
                new Function<String, FemaleInfo>() {
                    @Override
                    public FemaleInfo call(String line) throws Exception {

                        //以“，”为标识进行分割
                        String[] parts = line.split(",");

                        FemaleInfo femaleInfo = new FemaleInfo();
                        femaleInfo.setName(parts[0]);
                        femaleInfo.setGender(parts[1]);
                        femaleInfo.setStayTime(Integer.parseInt(parts[2].trim()));
                        return femaleInfo;
                    }
                });

        //注册表。从的list中创建DataFrame:
        DataFrame schemaFemaleInfo = sqlContext.createDataFrame(femaleInfoJavaRDD,FemaleInfo.class);

        // DataFrame使用给定名称将其注册为临时表。
        schemaFemaleInfo.registerTempTable("FemaleInfoTable");

		// 执行SQL查询操作，
        DataFrame femaleTimeInfo = sqlContext.sql("select * from " +
                "(select name,sum(stayTime) as totalStayTime from FemaleInfoTable " +
                "where gender = 'female' group by name )" +
                " tmp where totalStayTime >120");

       // 收集结果中的行的列。
        List<String> result = femaleTimeInfo.javaRDD().map(new Function<Row, String>() {
            public String call(Row row) {
                return  row.getString(0) + "," + row.getLong(1);
            }
        }).collect();
        System.out.println(result);
        jsc.stop();
    }
}
