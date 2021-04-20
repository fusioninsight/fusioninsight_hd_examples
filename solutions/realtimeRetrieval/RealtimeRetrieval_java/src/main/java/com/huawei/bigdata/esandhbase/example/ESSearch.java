package com.huawei.bigdata.esandhbase.example;

import com.google.gson.Gson;

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
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.hwclient.LoginUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ESSearch {
    static {
        //日志配置文件
        PropertyConfigurator.configure(ESSearch.class.getClassLoader().getResource("log4j.properties").getPath());
    }

    private static Properties properties = new Properties();

    private static final Logger LOG = LoggerFactory.getLogger(ESSearch.class);

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

    private static String principal;

    private static String schema = "https";

    private static RestClientBuilder builder = null;

    private static RestClient restClient = null;

    public static void main(String[] args) throws Exception {

        init();
        restClient = getRestClient();

        System.out.println(exist(restClient, "huawei"));

        query(restClient, "huawei", "doc", "灵雁");
        //在进行完Elasticsearch操作后，需要调用“restClient.close()”关闭所申请的资源。
        if (restClient != null) {
            try {
                restClient.close();
                LOG.info("Close the client successful in main.");
            } catch (Exception e1) {
                LOG.error("Close the client failed in main.", e1);
            }
        }
    }

    public static void init() throws Exception {
        properties.load(
            new FileInputStream(ESSearch.class.getClassLoader().getResource("es-example.properties").getPath()));
        //EsServerHost in es-example.properties must as ip1:port1,ip2:port2,ip3:port3....  eg:1.1.1.1:24100,2.2.2.2:24102,3.3.3.3:24100
        esServerHost = properties.getProperty("EsServerHost");
        MaxRetryTimeoutMillis = Integer.valueOf(properties.getProperty("MaxRetryTimeoutMillis"));
        ConnectTimeout = Integer.valueOf(properties.getProperty("ConnectTimeout"));
        SocketTimeout = Integer.valueOf(properties.getProperty("SocketTimeout"));
        isSecureMode = properties.getProperty("isSecureMode");
        index = properties.getProperty("index");
        type = properties.getProperty("type");
        id = Integer.valueOf(properties.getProperty("id"));
        shardNum = Integer.valueOf(properties.getProperty("shardNum"));
        replicaNum = Integer.valueOf(properties.getProperty("replicaNum"));
        principal = properties.getProperty("principal");

        //安全登录
        if ((isSecureMode).equals("true")) {
            String krb5ConfFile = ESSearch.class.getClassLoader().getResource("krb5.conf").getPath().substring(1);
            String jaasPath = ESSearch.class.getClassLoader().getResource("jaas.conf").getPath().substring(1);

            System.setProperty("java.security.krb5.conf", krb5ConfFile);
            System.setProperty("java.security.auth.login.config", jaasPath);
            System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
            System.setProperty("es.security.indication", "true");
            LOG.info("es.security.indication is  " + System.getProperty("es.security.indication"));
        } else if ((isSecureMode).equals("false")) {
            System.setProperty("es.security.indication", "false");
            schema = "http";
        }
    }

    //************* 获取客户端，连接Elasticsearch集群 ************
    public static RestClient getRestClient() throws Exception {
        List<HttpHost> hosts = new ArrayList<HttpHost>();
        String[] hostArray1 = esServerHost.split(",");

        for (String host : hostArray1) {
            String[] ipPort = host.split(":");
            HttpHost hostNew = new HttpHost(ipPort[0], Integer.valueOf(ipPort[1]), schema);
            hosts.add(hostNew);
        }
        HttpHost[] httpHosts = hosts.toArray(new HttpHost[] {});
        builder = RestClient.builder(httpHosts);
        builder = builder.setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
            @Override
            public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder) {
                return requestConfigBuilder.setConnectTimeout(ConnectTimeout).setSocketTimeout(SocketTimeout);
            }
        }).setMaxRetryTimeoutMillis(MaxRetryTimeoutMillis);
        Header[] defaultHeaders = new Header[] {
            new BasicHeader("Accept", "application/json"), new BasicHeader("Content-type", "application/json")
        };
        builder.setDefaultHeaders(defaultHeaders);
        restClient = builder.build();
        LOG.info("The RestClient has been created !");
        restClient.setHosts(httpHosts);
        return restClient;
    }

    //*************创建指定分片数目的索引************
    public static void createIndex(String indexName) {
        Response rsp = null;
        Map<String, String> params = Collections.singletonMap("pretty", "true");
        String jsonString = "{" + "\"settings\":{" + "\"number_of_shards\":\"" + shardNum + "\","
            + "\"number_of_replicas\":\"" + replicaNum + "\"" + "}}";
        HttpEntity entity = new NStringEntity(jsonString, ContentType.APPLICATION_JSON);
        Request request = new Request("PUT", "/" + indexName);
        request.setEntity(entity);
        request.addParameters(params);
        try {
            rsp = restClient.performRequest(request);
            if (HttpStatus.SC_OK == rsp.getStatusLine().getStatusCode()) {
                LOG.info("CreateIndexWithShardNum successful.");
            } else {
                LOG.error("CreateIndexWithShardNum failed.");
            }
            LOG.info("CreateIndexWithShardNum response entity is : " + EntityUtils.toString(rsp.getEntity()));
        } catch (Exception e) {
            LOG.error("CreateIndexWithShardNum failed, exception occurred.", e);
        }
    }

    //检查指定的索引是否存在于Elasticsearch集群中。
    public static boolean exist(RestClient restClient, String index) {
        Map<String, String> params = Collections.singletonMap("pretty", "true");
        boolean isExist = true;
        Response rsp = null;
        try {
            Request request = new Request("HEAD", "/" + index);
            request.addParameters(params);
            rsp = restClient.performRequest(request);
            if (HttpStatus.SC_OK == rsp.getStatusLine().getStatusCode()) {
                LOG.info("Check index successful,index is exist : " + index);
            }
            if (HttpStatus.SC_NOT_FOUND == rsp.getStatusLine().getStatusCode()) {
                LOG.info("index is not exist : " + index);
                isExist = false;
            }

        } catch (Exception e) {
            LOG.error("Check index failed, exception occurred.", e);
        }
        return isExist;
    }

    public static void query(RestClient restClientTest, String index, String type, String queryContent) {
        Response rsp = null;
        String jsonString = "{" + "\"query\":{" + "\"match\":{" + "\"name\":\"" + queryContent + "\"" + "}" + "}" + "}";

        Map<String, String> params = Collections.singletonMap("pretty", "true");
        HttpEntity entity = new NStringEntity(jsonString, ContentType.APPLICATION_JSON);
        try {
            Request request = new Request("GET", "/" + index + "/" + type + "/_search");
            request.setEntity(entity);
            request.addParameters(params);
            rsp = restClientTest.performRequest(request);
            if (HttpStatus.SC_OK == rsp.getStatusLine().getStatusCode()) {
                LOG.info("QueryData successful.");
            } else {
                LOG.error("QueryData failed.");
            }
            LOG.info("QueryData response entity is : " + EntityUtils.toString(rsp.getEntity()));
        } catch (Exception e) {
            LOG.error("QueryData failed, exception occurred.", e);
        }
    }

    //*************添加索引内容************
    //添加索引(姓名,地址,时间)
    public static void putData(RestClient restClientTest, String id, String name, String address, String date) {
        StringBuffer buffer = new StringBuffer();
        HttpEntity entity = null;
        Gson gson = new Gson();
        Map<String, Object> esMap = new HashMap<String, Object>();
        esMap.clear();
        esMap.put("id", id);
        esMap.put("name", name);
        esMap.put("address", address);
        esMap.put("date", date);
        String strJson = gson.toJson(esMap);
        buffer.append(strJson).append("\n");
        entity = new NStringEntity(buffer.toString(), ContentType.APPLICATION_JSON);

        Map<String, String> params = Collections.singletonMap("pretty", "true");
        Response rsp = null;
        try {
            Request request = new Request("POST", "/" + index + "/" + type + "/" + id);
            request.setEntity(entity);
            request.addParameters(params);
            rsp = restClientTest.performRequest(request);
            if (HttpStatus.SC_OK == rsp.getStatusLine().getStatusCode() || HttpStatus.SC_CREATED == rsp.getStatusLine()
                .getStatusCode()) {
                LOG.info("PutData successful." + buffer.toString());
            } else {
                LOG.error("PutData failed.");
            }
            LOG.info("PutData response entity is : " + EntityUtils.toString(rsp.getEntity()));
        } catch (Exception e) {
            LOG.error("PutData failed, exception occurred.", e);
        }

    }

    //****************批量写入数据**********************
    //批量写入数据到指定的索引中。esMap中添加的数据即为需要写入索引的数据信息
    public static void bulk() {
        Map<String, String> params = Collections.singletonMap("pretty", "true");
        Long oneCommit = 5L;//请根据实际需要修改每个循环中提交的索引文档的数量。
        Long totalRecordNumber = 10L;//请根据实际需要修改此批量请求方法中需要提交的索引文档总数。
        Long circleNumber = totalRecordNumber / oneCommit;
        HttpEntity entity2 = null;
        Gson gson = new Gson();
        Map<String, Object> esMap = new HashMap<String, Object>();
        String str = "{ \"index\" : { \"_index\" : \"" + index + "\", \"_type\" :  \"" + type + "\"} }";

        for (int i = 1; i <= circleNumber; i++) {
            Request request = new Request("PUT", "/_bulk");
            StringBuffer buffer = new StringBuffer();
            for (int j = 1; j <= oneCommit; j++) {
                esMap.clear();

                esMap.put("id", "id");
                esMap.put("name", "name");
                esMap.put("address", "address");
                esMap.put("date", "date");

                String strJson = gson.toJson(esMap);
                buffer.append(str).append("\n");
                buffer.append(strJson).append("\n");
            }
            entity2 = new NStringEntity(buffer.toString(), ContentType.APPLICATION_JSON);
            Response rsp5 = null;
            try {
                request.setEntity(entity2);
                request.addParameters(params);
                rsp5 = restClient.performRequest(request);
                if (HttpStatus.SC_OK == rsp5.getStatusLine().getStatusCode()) {
                    LOG.info("Bulk successful.");
                    LOG.info("Already input documents : " + oneCommit * i);
                } else {
                    LOG.error("Bulk failed.");
                }
                LOG.info("Bulk response entity is : " + EntityUtils.toString(rsp5.getEntity()));
            } catch (Exception e) {
                LOG.error("Bulk failed, exception occurred.", e);
            }
        }
    }
}