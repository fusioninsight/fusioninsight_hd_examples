package com.huawei.bigdata;

import com.huawei.bigdata.body.*;

import com.huawei.bigdata.information.HttpAuthInfo;
import com.huawei.bigdata.information.RestApi;
import com.huawei.bigdata.information.WebHcatHttpClient;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

public class WebHcat {
    static {
        PropertyConfigurator.configure(WebHcat.class.getClassLoader().getResource("log4j.properties").getPath());
    }

    private static final Logger LOG = LoggerFactory.getLogger(WebHcat.class);
    private static final String DEFAULT_CONFIG_FILE = "webHcat.properties";

    public static void main(String[] args) {
        String user = "lyysxg@HADOOP1.COM";
        String keytab = "C:\\Users\\zwx613270\\Desktop\\WebHcat\\src\\main\\resources\\user.keytab";
        String krb5Location = "C:\\Users\\zwx613270\\Desktop\\WebHcat\\src\\main\\resources\\krb5.conf";

        InputStream inputStream = null;
        HttpAuthInfo httpAuthInfo = null;
        try {
            inputStream = new FileInputStream(WebHcat.class.getClassLoader().getResource(DEFAULT_CONFIG_FILE).getPath());
            Properties p = new Properties();
            p.load(inputStream);
            httpAuthInfo = HttpAuthInfo.newBuilder().setIp(p.getProperty("ip"))
                    .setPort(Integer.valueOf(p.getProperty("port")))
                    .build();
            WebHcatHttpClient client = WebHcatHttpClient.getClient(httpAuthInfo);
            RestApi api = new RestApi(client);
            api.login(user, keytab, krb5Location);
            //列出所有的数据库
            api.searchPath();
            ReqDatabaseBody reqDatabaseBody1 = new ReqDatabaseBody();
            reqDatabaseBody1.setName("db2");
                //获取指定数据库的详细信息
            //注：未对不存在的表进行判别。
            api.searchPath(reqDatabaseBody1);
            //创建数据库

            ReqCreateDataBaBody createDataBaBody = new ReqCreateDataBaBody();
            createDataBaBody.setLocation("/zwl/dataBase11111");
            createDataBaBody.setComment("my db");
            HashMap<String, String> CreateData = new HashMap<>();
            CreateData.put("a", "b");
            createDataBaBody.setProperties(CreateData);
            api.CreateDataBase(createDataBaBody, "Test1");
            //创建表
            ReqCreateTableBody reqCreateTableBody = new ReqCreateTableBody();
            reqCreateTableBody.setComment("Best table made today");
            List<HashMap<String, String>> list = new ArrayList<>();
            HashMap<String, String> hashMap = new HashMap<>();
            hashMap.put("name", "id");
            hashMap.put("type", "bigint");
            list.add(hashMap);
            HashMap hashMap2 = new HashMap();
            hashMap2.put("name", "price");
            hashMap2.put("type", "float");
            hashMap2.put("comment", "The unit price");
            list.add(hashMap2);
            reqCreateTableBody.setColumns(list);
            HashMap hashMap1 = new HashMap();
            hashMap1.put("name", "country");
            hashMap1.put("type", "string");
            List<HashMap<String, String>> list1 = new ArrayList<>();
            list1.add(hashMap1);
            reqCreateTableBody.setPartitionedBy(list1);
            HashMap<String, Object> clusteredBy = new HashMap();
            List<Object> list2 = new ArrayList<>();
            list2.add("id");
            clusteredBy.put("columnNames", list2);
            List<Object> list3 = new ArrayList<>();
            HashMap<String, String> hashMap3 = new HashMap<>();
            hashMap3.put("columnName", "id");
            hashMap3.put("order", "ASC");
            list3.add(hashMap3);
            clusteredBy.put("sortedBy", list3);
            clusteredBy.put("numberOfBuckets", "10");
            reqCreateTableBody.setClusteredBy(clusteredBy);
            HashMap<String, Object> hashMap4 = new HashMap<>();
            hashMap4.put("storedAs", "rcfile");
            HashMap hashMap5 = new HashMap();
            hashMap5.put("key", "value");
            HashMap hashMap6 = new HashMap();
            hashMap6.put("properties", hashMap5);
            hashMap6.put("name", "org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe");
            HashMap hashMap7 = new HashMap();
            hashMap7.put("serde", hashMap6);
            hashMap7.put("fieldsTerminatedBy", "\u0001");
            hashMap4.put("rowFormat", hashMap7);
            reqCreateTableBody.setFormat(hashMap4);
            api.CreateTable(reqCreateTableBody, "Test1", "lyysxg1");
            //查询数据库中的所有表
            ReqSearchTable searchTable = new ReqSearchTable();
            searchTable.setDataBaseName("Test1");
            api.searchTable(searchTable);
            //获取表的详细信息
            ReqTabelInfoBody tabelInfoBody = new ReqTabelInfoBody();
            tabelInfoBody.setDataName("Test1");
            tabelInfoBody.setTableName("lyysxg1");
            tabelInfoBody.setFormat("extended");
            api.SearchTableInfo(tabelInfoBody);
            //重命名表
            api.RenameTable("Test1", "lyysxg1", "zwllyysxg");
            //删除表
            ReqDelTableBody reqDelTableBody = new ReqDelTableBody();
            reqDelTableBody.setDataBaseName("Test1");
            reqDelTableBody.setTabelName("zwllyysxg");
            reqDelTableBody.setIfExists("true");
            api.DeleteTable(reqDelTableBody);
            //删除数据库
            DeleteDataBaseBody delete = new DeleteDataBaseBody();
            delete.setName("Test1");
            delete.setIfExists("true");
            delete.setOption("cascade");
            api.DeleteDatabase(delete);

            System.out.println();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
