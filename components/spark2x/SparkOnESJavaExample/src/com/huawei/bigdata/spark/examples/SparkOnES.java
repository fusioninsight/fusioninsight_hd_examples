package com.huawei.bigdata.spark.examples;

import org.apache.http.nio.entity.NStringEntity;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.apache.hadoop.conf.Configuration;


public class SparkOnES {
    private static final Logger LOG = LoggerFactory.getLogger(SparkOnES.class);
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
    private static String snifferEnable;
    private static String schema = "https";
    private static RestClient restClient = null;
    private static String principal;


    public static void main(String args[]) throws Exception{
        LOG.info("Start to run the spark on ES test!");
        //login with keytab
        String userPrincipal = "super";
        String userKeytabPath = System.getProperty("user.dir") + File.separator + "user.keytab";
        String krb5ConfPath = System.getProperty("user.dir") + File.separator +"krb5.conf";
        //generate jaas file, which is in the same dir with keytab file
        LoginUtil.setJaasFile(userPrincipal,userKeytabPath);
        Configuration hadoopConf = new Configuration();
        LoginUtil.login(userPrincipal, userKeytabPath, krb5ConfPath, hadoopConf);

        initProperties();
        try {
            restClient = getRestClient(getHostArray(esServerHost));
            index = "people";
            type = "doc";
            //check whether the index to be added has already exist, remove the exist one
            if (exist(index)) {
                delete(index);
            }
            createIndexWithShardNum(index);

            SparkConf conf = new SparkConf().setAppName("SparkOnES");
            JavaSparkContext jsc = new JavaSparkContext(conf);
            JavaRDD<String> inputs = jsc.textFile("/spark-on-es/people.json");
            List<String> jsonList = inputs.collect();
            for (int i = 0; i < jsonList.size(); i++) {
                putData(index, type, Integer.valueOf(i).toString(), jsonList.get(i).toString());
            }
            List<String> resultList = new ArrayList<String>();
            for (int i=0; i< jsonList.size(); i++){
                resultList.add(queryData(index, type, Integer.valueOf(i).toString()));
            }
            JavaRDD<String> resultRDD = jsc.parallelize(resultList);
            long count = resultRDD.count();
            System.out.println("The total count is:"  + count);
            jsc.stop();

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


    /*
    * initialize basic configurations for elasticsearch
     * */
    private static void initProperties() throws Exception {
        Properties properties = new Properties();
        String path = System.getProperty("user.dir") + File.separator
                + "es-example.properties";
        path = path.replace("\\", "\\\\");

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
        type = properties.getProperty("type");
        id = Integer.valueOf(properties.getProperty("id"));
        shardNum = Integer.valueOf(properties.getProperty("shardNum"));
        replicaNum = Integer.valueOf(properties.getProperty("replicaNum"));
        principal = properties.getProperty("principal");

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
        LOG.info("principal:" + principal);
    }

    /**
     * Get hostArray by esServerHost of cluster in property file
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
     * Get one rest client instance.
     * @return
     * @throws Exception
     */
    private static RestClientBuilder getRestClientBuilder(HttpHost[] HostArray) throws Exception {

        RestClientBuilder builder = null;
        if (isSecureMode.equals("true")) {
            System.setProperty("es.security.indication", "true");
            System.setProperty("elasticsearch.kerberos.jaas.appname", "EsClient");
            builder = RestClient.builder(HostArray);
        } else {
            System.setProperty("es.security.indication", "false");
            builder = RestClient.builder(HostArray);
        }
        Header[] defaultHeaders = new Header[] { new BasicHeader("Accept", "application/json"),
                new BasicHeader("Content-type", "application/json") };
        builder.setDefaultHeaders(defaultHeaders);
        builder.setMaxRetryTimeoutMillis(MaxRetryTimeoutMillis);
        builder.setFailureListener(new RestClient.FailureListener(){
            public void onFailure(HttpHost host){
                //trigger some actions when failure occurs
            }
        });
        builder.setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback(){

            public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder) {
                return requestConfigBuilder.setConnectTimeout(ConnectTimeout).setSocketTimeout(SocketTimeout);
            }
        });

        return builder;
    }
    private static RestClient getRestClient(HttpHost[] HostArray) throws Exception {
        RestClient restClient = null;
        restClient = getRestClientBuilder(HostArray).build();
        LOG.info("The RestClient has been created !");
        restClient.setHosts(HostArray);
        return restClient;
    }


    /**
    * Check whether the index has already existed in ES
    * @param index
    * */
    public static boolean exist(String index){
        Response response = null;
        Map<String, String> params = Collections.singletonMap("pretty", "true");
        try{
            response = restClient.performRequest("HEAD", "/" + index , params);
            if(HttpStatus.SC_OK == response.getStatusLine().getStatusCode()){
                LOG.info("Ths index exist already:" + index);
                return true;
            }
            if(HttpStatus.SC_NOT_FOUND == response.getStatusLine().getStatusCode()){
                LOG.info("Index is not exist:" + index);
                return false;
            }
        }catch(Exception e){
            LOG.error("Fail to check index!", e);
        }
        return false;
    }

    /**
     * Create one index with shard number.
     * @param index
     * @throws Exception
     */
    private static void createIndexWithShardNum(String index) {
        Response rsp = null;
        Map<String, String> params = Collections.singletonMap("pretty", "true");
        String jsonString = "{" + "\"settings\":{" + "\"number_of_shards\":\"" + shardNum + "\","
                + "\"number_of_replicas\":\"" + replicaNum + "\"" + "}}";

        HttpEntity entity = new NStringEntity(jsonString, ContentType.APPLICATION_JSON);
        try {

            rsp = restClient.performRequest("PUT", "/" + index, params, entity);
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
     * @param index
     * @param jsonString
     */
    private static void putData(String index, String type, String id, String jsonString) {
        Map<String, String> params = Collections.singletonMap("pretty", "true");
        HttpEntity entity = new NStringEntity(jsonString, ContentType.APPLICATION_JSON);
        Response rsp = null;

        try {
            rsp = restClient.performRequest("POST", "/" + index + "/" + type +"/" + id , params, entity);
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
     * @param index
     * @param type
     * @param id:the id of the people for query
     */
    private static String queryData(String index, String type, String id) {
        Response rsp = null;
        Map<String, String> params = Collections.singletonMap("pretty", "true");
        try {
            rsp = restClient.performRequest("GET", "/" + index+ "/" + type+ "/" + id, params);
            if(HttpStatus.SC_OK == rsp.getStatusLine().getStatusCode()) {
                LOG.info("QueryData successful.");
            }
            else {
                LOG.error("QueryData failed.");
            }
            String result = EntityUtils.toString(rsp.getEntity());
            LOG.info("QueryData response entity is : " + result);
            return result;
        }catch (Exception e) {

            LOG.error("QueryData failed, exception occurred.",e);
            return "";
        }
    }

    /**
     * Delete one index
     * @param index
     */
    private static void delete(String index) {
        Response rsp = null;
        try {
            rsp = restClient.performRequest("DELETE", "/" + index + "?&pretty=true");
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

}
