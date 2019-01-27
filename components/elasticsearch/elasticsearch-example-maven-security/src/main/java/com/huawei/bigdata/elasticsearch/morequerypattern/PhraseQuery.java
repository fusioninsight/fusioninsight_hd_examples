package com.huawei.bigdata.elasticsearch.morequerypattern;

 import com.google.gson.Gson;
 import com.huawei.bigdata.elasticsearch.AESDemoForDoc;
 import org.apache.http.Header;
 import org.apache.http.HttpEntity;
 import org.apache.http.HttpHost;
 import org.apache.http.HttpStatus;
 import org.apache.http.client.config.RequestConfig;
 import org.apache.http.entity.ContentType;
 import org.apache.http.entity.StringEntity;
 import org.apache.http.message.BasicHeader;
 import org.apache.http.nio.entity.NStringEntity;
 import org.apache.http.util.EntityUtils;
 import org.apache.log4j.PropertyConfigurator;
 import org.elasticsearch.client.Response;
 import org.elasticsearch.client.RestClient;
 import org.elasticsearch.client.RestClientBuilder;
 import org.slf4j.Logger;
 import org.slf4j.LoggerFactory;

 import java.io.File;
 import java.io.FileInputStream;
 import java.util.*;

 public class  PhraseQuery {
     private static final Logger LOG = LoggerFactory.getLogger(AESDemoForDoc.class);
     private static String isSecureMode;
     private static String esServerHost;
     private static int MaxRetryTimeoutMillis;
     private static String index;
     private static String type;
     private static int id;
     private static int shardNum;
     private static int replicaNum;
     private static int ConnectTimeout;
     private static int SocketTimeout;
     private static String schema = "https";
     private static RestClientBuilder builder = null;
     private static RestClient restClient = null;

     static {
         //日志配置文件
         PropertyConfigurator.configure(AESDemoForDoc.class.getClassLoader().getResource("conf/log4j.properties").getPath());
     }

     public static void main(String[] args)  throws Exception{
         LOG.info("Start to do es test !");

         //加载es-example.properties配置文件中的内容，包括与主机的连接信息，索引信息等
         init();
         //安全登录
         secureAuth();
         //获取客户端
         getClient();;
         //批量写入测试数据
         // putTestData();
         //短语查询（精确匹配）
         phraseQuery();

         //关闭所申请的资源。
         destroy();
     }
     private static void phraseQuery(){
         //在请求中添加pretty参数，可以让ES输出为容易阅读的JSON格式数据
         Map<String, String> params = Collections.singletonMap("pretty", "true");

         //命令解释：查询title字段中带有”is divided into”的文档
         String jsonString = "{\"query\" : { \"match_phrase\": { \"title\" : \"is divided into\" }}}";

         HttpEntity entity = new NStringEntity(jsonString, ContentType.APPLICATION_JSON);
         Response rsp = null;
         try {
             rsp = restClient.performRequest("GET", "/" + index + "/_search" , params, entity);
             if(HttpStatus.SC_OK == rsp.getStatusLine().getStatusCode()||HttpStatus.SC_CREATED == rsp.getStatusLine().getStatusCode()) {
                 LOG.info("queryData successful.");
             }else {
                 LOG.error("queryData failed.");
             }
             LOG.info("queryData response entity is : " + EntityUtils.toString(rsp.getEntity()));
         } catch (Exception e) {
             LOG.error("queryData failed, exception occurred.",e);
         }
     }

     private static void putTestData(){
         //****************批量写入数据**********************
         String[] dataArray = {
                 "Our diurnal existence is divided into two phases",
                 "as distinct as day and night",
                 "We call them work and play",
                 "We work so many hours a day.",
                 "And, when we have allowed the necessary minimum for such activities as eating and shopping"
         };

         //在请求中添加pretty参数，可以让ES输出为容易阅读的JSON格式数据
         Map<String, String> params = Collections.singletonMap("pretty", "true");
         //批量写入数据到指定的索引中。esMap中添加的数据即为需要写入索引的数据信息
         Long idNumber = 0L;
         Long oneCommit = 5L;//请根据实际需要修改每个循环中提交的索引文档的数量。
         Long totalRecordNumber = 5L;//请根据实际需要修改此批量请求方法中需要提交的索引文档总数。
         Long circleNumber = totalRecordNumber/oneCommit;
         StringEntity entity = null;
         Gson gson = new Gson();
         Map<String,Object> esMap = new HashMap<String,Object>();
         String str = "{ \"index\" : { \"_index\" : \"" + index + "\", \"_type\" :  \"" + type + "\"} }";

         for (int i = 1; i <=circleNumber; i++) {
             StringBuffer buffer = new StringBuffer();
             for (int j = 1; j <= oneCommit; j++) {
                 esMap.clear();
                 esMap.put("title", dataArray[j-1]);
                 esMap.put("cur_time", new Date().getTime());

                 String strJson = gson.toJson(esMap);
                 buffer.append(str).append("\n");
                 buffer.append(strJson).append("\n");
             }
             entity = new StringEntity(buffer.toString(), ContentType.APPLICATION_JSON);
             entity.setContentEncoding("UTF-8");
             Response rsp = null;
             try {
                 //批量导入数据，使用路径使用 /_bulk
                 //整个批量请求都需要由接收到请求的节点加载到内存中，因此该请求越大，其他请求所能获得的内存就越少。
                 //批量请求的大小有一个最佳值，大于这个值，性能将不再提升，甚至会下降。它完全取决于硬件、文档的大小和复杂度、索引和搜索的负载的整体情况。
                 //一个好的办法是开始时将 1,000 到 5,000 个文档作为一个批次, 如果你的文档非常大，那么就减少批量的文档个数。一个好的批量大小在开始处理后所占用的物理大小约为 5-15 MB。
                 rsp = restClient.performRequest("PUT", "/_bulk" ,params ,entity);
                 if(HttpStatus.SC_OK == rsp.getStatusLine().getStatusCode()) {
                     LOG.info("Bulk successful.");
                 }
                 else {
                     LOG.error("Bulk failed.");
                 }
             }catch (Exception e) {
                 LOG.error("Bulk failed, exception occurred.",e);
             }
         }
     }

     private static void init()  throws Exception{
         //加载es-example.properties配置文件中的内容，包括与主机的连接信息，索引信息等
         Properties properties = new Properties();
         String path =  AESDemoForDoc.class.getClassLoader().getResource("conf/es-example.properties").getPath();

         try {
             properties.load(new FileInputStream(new File(path)));
         } catch (Exception e) {
             throw new Exception("Failed to load properties file : " + path);
         }

         //“esServerHost”为已安装Elasticsearch集群中任意节点 IP与该IP节点上已安装的任意Elasticsearch实例的HTTP端口组合的列表，
         // 形如“ip1：port1，ip2:port2,ip3:port3......”。
         // **不建议在ES Master提交业务，业务量过大导致ES Master崩溃**
         // 所以请配置ES Node对应的ip和端口，该端口值可以通过以下方式获取：FusionInsight Manager界面点击“服务管理 > Elasticsearch > 服务配置>参数类别:(需选择)全部配置>”，
         esServerHost = properties.getProperty("EsServerHost");
         MaxRetryTimeoutMillis = Integer.valueOf(properties.getProperty("MaxRetryTimeoutMillis"));
         ConnectTimeout = Integer.valueOf(properties.getProperty("ConnectTimeout"));
         SocketTimeout = Integer.valueOf(properties.getProperty("SocketTimeout"));
         isSecureMode = properties.getProperty("isSecureMode");
         index = properties.getProperty("index");
         type = "WordQueryType";
         id = Integer.valueOf(properties.getProperty("id"));
         shardNum = Integer.valueOf(properties.getProperty("shardNum"));
         replicaNum = Integer.valueOf(properties.getProperty("replicaNum"));

         LOG.info("EsServerHost:" + esServerHost);
         LOG.info("MaxRetryTimeoutMillis:" + MaxRetryTimeoutMillis);
         LOG.info("ConnectTimeout:" + ConnectTimeout);
         LOG.info("SocketTimeout:" + SocketTimeout);
         LOG.info("index:" + index);
         LOG.info("shardNum:" + shardNum);
         LOG.info("replicaNum:" + replicaNum);
         LOG.info("isSecureMode:" + isSecureMode);
         LOG.info("type:" + type);
         LOG.info("id:" + id);
     }
     private static void destroy(){
         //在进行完Elasticsearch操作后，需要调用“restClient.close()”关闭所申请的资源。
         if( restClient!=null) {
             try {
                 restClient.close();
                 LOG.info("Close the client successful in main.");
             } catch (Exception e1) {
                 LOG.error("Close the client failed in main.",e1);
             }
         }
     }
     private static void secureAuth(){
         //安全登录
         if ((isSecureMode).equals("true")) {
             //加载krb5.conf：客户端与Kerberos对接的配置文件，配置到JVM系统参数中
             String krb5ConfFile = AESDemoForDoc.class.getClassLoader().getResource("conf/krb5.conf").getPath();
             LOG.info("krb5ConfFile: " + krb5ConfFile);
             System.setProperty("java.security.krb5.conf", krb5ConfFile);
             //加载jaas文件进行认证，并配置到JVM系统参数中
             String jaasPath = AESDemoForDoc.class.getClassLoader().getResource("conf/jaas.conf").getPath();
             LOG.info("jaasPath: " + jaasPath);
             System.setProperty("java.security.auth.login.config", jaasPath);
             System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
             //添加ES安全指示
             System.setProperty("es.security.indication", "true");
             LOG.info("es.security.indication is  " + System.getProperty("es.security.indication"));
         }else if((isSecureMode).equals("false")){
             System.setProperty("es.security.indication", "false");
             schema="http";
         }
     }
     private static void getClient(){
         //************* 获取客户端，连接Elasticsearch集群 ************
         //我们需要获取RestClient类 通过设置IP和端口连接到特定Elasticsearch集群
         //RestClient实例可以通过相应的RestClientBuilder类构建，该类通过RestClient＃builder（HttpHost ...）静态方法创建。
         // 唯一必需的参数是客户端将与之通信的一个或多个主机，作为HttpHost的实例提供。
         //HttpHost保存与主机的HTTP连接所需的所有变量。这包括远程主机名，端口和方案。
         List<HttpHost> hosts=new ArrayList<HttpHost>();
         String[] hostArray1 = esServerHost.split(",");

         for(String host:hostArray1) {
             String[] ipPort= host.split(":");
             HttpHost hostNew =new HttpHost(ipPort[0],Integer.valueOf(ipPort[1]),schema);
             hosts.add(hostNew);
         }
         HttpHost[] httpHosts = hosts.toArray(new HttpHost[] {});
         builder = RestClient.builder(httpHosts);
         // 设置请求的回调函数
         //1.设置连接超时时间，单位毫秒。
         //2.设置请求获取数据的超时时间，单位毫秒。如果访问一个接口，多少时间内无法返回数据，就直接放弃此次调用。
         //3.设置同一请求最大超时重试时间（以毫秒为单位）。
         builder = builder.setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
             @Override
             public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder) {
                 return requestConfigBuilder.setConnectTimeout(ConnectTimeout).setSocketTimeout(SocketTimeout);
             }
         }).setMaxRetryTimeoutMillis(MaxRetryTimeoutMillis);

         //设置默认请求标头，它将与每个请求一起发送。请求时标头将始终覆盖任何默认标头。
         Header[] defaultHeaders = new Header[] { new BasicHeader("Accept", "application/json"),
                 new BasicHeader("Content-type", "application/json") };
         builder.setDefaultHeaders(defaultHeaders);
         //根据配置好的RestClientBuilder创建新的RestClient。
         restClient = builder.build();
         LOG.info("The RestClient has been created !");
         restClient.setHosts(httpHosts);
     }

 }
