package com.huawei.bigdata.elasticsearch;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
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
import com.google.gson.Gson;


public class RestClientTest {
    static {
        //日志配置文件
        PropertyConfigurator.configure(RestClientTest.class.getClassLoader().getResource("conf/log4j.properties").getPath());
    }
    private static final Logger LOG = LoggerFactory.getLogger(RestClientTest.class);
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
    private static RestClient restClient = null;

    private static void initProperties() throws Exception {
        Properties properties = new Properties();
        String path =  RestClientTest.class.getClassLoader().getResource("conf/es-example.properties").getPath();

        try {
            properties.load(new FileInputStream(new File(path)));
        } catch (Exception e) {
            throw new Exception("Failed to load properties file : " + path);
        }
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

    /**
     * Get hostArray by esServerHost in property file
     * @param esServerHost
     * @return
     */
    public static HttpHost[] getHostArray(String esServerHost) throws Exception {

        if(("false").equals(isSecureMode)) {
            schema="http";
        }
        List<HttpHost> hosts=new ArrayList<HttpHost>();
        String[] hostArray1 = esServerHost.split(",");

        for(String host:hostArray1) {
            String[] ipPort= host.split(":");
            HttpHost hostNew =new HttpHost(ipPort[0],Integer.valueOf(ipPort[1]),schema);
            hosts.add(hostNew);
        }
        return hosts.toArray(new HttpHost[] {});
    }

    /**
     * Set configurations about authentication.
     * @throws Exception
     */
    private static void setSecConfig() throws Exception {

        String krb5ConfFile =  RestClientTest.class.getClassLoader().getResource("conf/krb5.conf").getPath();
        LOG.info("krb5ConfFile: " + krb5ConfFile);
        System.setProperty("java.security.krb5.conf", krb5ConfFile);

        String jaasPath =  RestClientTest.class.getClassLoader().getResource("conf/jaas.conf").getPath();

        LOG.info("jaasPath: " + jaasPath);
        System.setProperty("java.security.auth.login.config", jaasPath);
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");

        //add for ES security indication
        System.setProperty("es.security.indication", "true");
        LOG.info("es.security.indication is  " + System.getProperty("es.security.indication"));

    }

    /**
     * Get one rest client instance.
     * @return
     * @throws Exception
     */
    private static RestClient getRestClient(HttpHost[] HostArray) throws Exception {

        RestClientBuilder builder = null;
        RestClient restClient = null;
        if (isSecureMode.equals("true")) {
            setSecConfig();
            builder = RestClient.builder(HostArray);
        }
        else {
            System.setProperty("es.security.indication", "false");
            builder = RestClient.builder(HostArray);
        }
        Header[] defaultHeaders = new Header[] { new BasicHeader("Accept", "application/json"),
                new BasicHeader("Content-type", "application/json") };

        builder = builder.setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
            @Override
            public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder) {
                return requestConfigBuilder.setConnectTimeout(ConnectTimeout).setSocketTimeout(SocketTimeout);
            }
        }).setMaxRetryTimeoutMillis(MaxRetryTimeoutMillis);

        builder.setDefaultHeaders(defaultHeaders);
        restClient = builder.build();
        LOG.info("The RestClient has been created !");
        restClient.setHosts(HostArray);

        return restClient;
    }

    /**
     * Query the cluster's healt
     */

    private static void queryEsClusterHealth(RestClient restClientTest) {
        Response rsp = null;
        Map<String, String> params = Collections.singletonMap("pretty", "true");

        try {
            rsp = restClientTest.performRequest("GET", "/_cluster/health" , params);
            if(HttpStatus.SC_OK == rsp.getStatusLine().getStatusCode()) {
                LOG.info("QueryEsClusterHealth successful.");
            }else {
                LOG.error("QueryEsClusterHealth failed.");
            }
            LOG.info("QueryEsClusterHealth response entity is : " + EntityUtils.toString(rsp.getEntity()));
        }catch (Exception e) {
            LOG.error("QueryEsClusterHealth failed, exception occurred.",e);
        }

    }

    /**
     * Check the existence of the index or not.
     */
    private static boolean exist(RestClient restClientTest) {
        Response rsp = null;
        Map<String, String> params = Collections.singletonMap("pretty", "true");
        try {
            rsp = restClientTest.performRequest("HEAD", "/" + index , params);
            if(HttpStatus.SC_OK == rsp.getStatusLine().getStatusCode()) {
                LOG.info("Check index successful,index is exist : " + index);
                return true;
            }
            if(HttpStatus.SC_NOT_FOUND == rsp.getStatusLine().getStatusCode()){
                LOG.info("index is not exist : " + index);
                return false;
            }

        } catch (Exception e) {
            LOG.error("Check index failed, exception occurred.",e);
        }
        return false;
    }

    /**
     * Create one index with shard number.
     * @throws Exception
     * @throws IOException
     */
    private static void createIndexWithShardNum(RestClient restClientTest) {
        Response rsp = null;
        Map<String, String> params = Collections.singletonMap("pretty", "true");
        String jsonString = "{" + "\"settings\":{" + "\"number_of_shards\":\"" + shardNum + "\","
                + "\"number_of_replicas\":\"" + replicaNum + "\"" + "}}";

        HttpEntity entity = new NStringEntity(jsonString, ContentType.APPLICATION_JSON);
        try {

            rsp = restClientTest.performRequest("PUT", "/" + index, params, entity);
            if(HttpStatus.SC_OK == rsp.getStatusLine().getStatusCode()) {
                LOG.info("CreateIndexWithShardNum successful.");
            }else {
                LOG.error("CreateIndexWithShardNum failed.");
            }
            LOG.info("CreateIndexWithShardNum response entity is : " + EntityUtils.toString(rsp.getEntity()));
        }catch (Exception e) {
            LOG.error("CreateIndexWithShardNum failed, exception occurred.",e);
        }

    }


    /**
     * Put one document into the index
     */

    private static void putData(RestClient restClientTest) {
        String jsonString = "{" + "\"name\":\"Happy\"," + "\"author\":\"Alex Yang \","
                + "\"pubinfo\":\"Beijing,China. \"," + "\"pubtime\":\"2016-07-16\","
                + "\"desc\":\"Elasticsearch is a highly scalable open-source full-text search and analytics engine.\""
                + "}";

        Map<String, String> params = Collections.singletonMap("pretty", "true");
        HttpEntity entity = new NStringEntity(jsonString, ContentType.APPLICATION_JSON);
        Response rsp = null;

        try {
            rsp = restClientTest.performRequest("POST", "/" + index + "/" + type +"/" + id , params, entity);
            if(HttpStatus.SC_OK == rsp.getStatusLine().getStatusCode()||HttpStatus.SC_CREATED == rsp.getStatusLine().getStatusCode()) {
                LOG.info("PutData successful.");
            }else {
                LOG.error("PutData failed.");
            }
            LOG.info("PutData response entity is : " + EntityUtils.toString(rsp.getEntity()));
        } catch (Exception e) {
            LOG.error("PutData failed, exception occurred.",e);
        }

    }
    /**
     * Query all data of one index.
     */
    private static void queryData(RestClient restClientTest) {
        Response rsp = null;
        Map<String, String> params = Collections.singletonMap("pretty", "true");
        try {
            rsp = restClientTest.performRequest("GET", "/" + index+ "/" + type+ "/" + id, params);
            if(HttpStatus.SC_OK == rsp.getStatusLine().getStatusCode()) {
                LOG.info("QueryData successful.");
            }
            else {
                LOG.error("QueryData failed.");
            }
            LOG.info("QueryData response entity is : " + EntityUtils.toString(rsp.getEntity()));
        }catch (Exception e) {
            LOG.error("QueryData failed, exception occurred.",e);
        }
    }

    /**
     * Delete one index
     */
    private static void delete(RestClient restClientTest) {
        Response rsp = null;
        try {
            rsp = restClientTest.performRequest("DELETE", "/" + index + "?&pretty=true");
            if(HttpStatus.SC_OK == rsp.getStatusLine().getStatusCode()) {
                LOG.info("Delete successful.");
            }
            else {
                LOG.error("Delete failed.");
            }
            LOG.info("Delete response entity is : " + EntityUtils.toString(rsp.getEntity()));
        }catch (Exception e) {
            LOG.error("Delete failed, exception occurred.",e);
        }
    }

    /**
     * Send a bulk request
     * @param restClientTest
     */
    private static void bulk(RestClient restClientTest) {

        Long idNumber = 0L;
        Long oneCommit = 5L;//请根据实际需要修改每个循环中提交的索引文档的数量。
        Long totalRecordNumber = 10L;//请根据实际需要修改此批量请求方法中需要提交的索引文档总数。
        Long circleNumber = totalRecordNumber/oneCommit;
        StringEntity entity = null;
        Gson gson = new Gson();
        Map<String,Object> esMap = new HashMap<String,Object>();
        String str = "{ \"index\" : { \"_index\" : \"" + index + "\", \"_type\" :  \"" + type + "\"} }";

        for (int i = 1; i <=circleNumber; i++) {
            StringBuffer buffer = new StringBuffer();
            for (int j = 1; j <= oneCommit; j++) {
                esMap.clear();
                idNumber = Long.valueOf(idNumber.longValue() + 1L);
                esMap.put("id_number", idNumber);
                esMap.put("name", "Linda");
                esMap.put("age", ThreadLocalRandom.current().nextInt(18, 100));
                esMap.put("height", (float) ThreadLocalRandom.current().nextInt(140, 220));
                esMap.put("weight", (float) ThreadLocalRandom.current().nextInt(70, 200));
                esMap.put("cur_time", new Date().getTime());

                String strJson = gson.toJson(esMap);
                buffer.append(str).append("\n");
                buffer.append(strJson).append("\n");
            }
            entity = new StringEntity(buffer.toString(), ContentType.APPLICATION_JSON);
            entity.setContentEncoding("UTF-8");
            Response rsp = null;
            Map<String, String> params = Collections.singletonMap("pretty", "true");
            try {
                rsp = restClientTest.performRequest("PUT", "/_bulk" ,params ,entity);
                if(HttpStatus.SC_OK == rsp.getStatusLine().getStatusCode()) {
                    LOG.info("Bulk successful.");
                    LOG.info("Already input documents : " + oneCommit*i);
                }
                else {
                    LOG.error("Bulk failed.");
                }
                LOG.info("Bulk response entity is : " + EntityUtils.toString(rsp.getEntity()));
            }catch (Exception e) {
                LOG.error("Bulk failed, exception occurred.",e);
            }
        }
    }


    public static void main(String[] args) throws Exception {

        LOG.info("Start to do es test !");

        initProperties();
        try {
            restClient = getRestClient(getHostArray(esServerHost));
            queryEsClusterHealth(restClient);
            if (exist(restClient)) {
                delete(restClient);
            }
            createIndexWithShardNum(restClient);
            putData(restClient);
            bulk(restClient);
            queryData(restClient);

        } catch(Exception e) {
            LOG.error("There are exceptions in main.",e);
        } finally {
            if( restClient!=null) {
                try {
                    restClient.close();
                    LOG.info("Close the client successful in main.");
                } catch (Exception e1) {
                    LOG.error("Close the client failed in main.",e1);
                }
            }
        }
    }
}


