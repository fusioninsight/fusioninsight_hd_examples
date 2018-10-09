package com.huawei.bigdata.elasticsearch.examples;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicHeader;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.junit.Assert;
//import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestClientTest
{

    private static final Logger LOG = LoggerFactory.getLogger(RestClientTest.class);

    private static String isSecureMode;

    private static String esClientIP;

    private static int esClientIPPort;

    private static int MaxRetryTimeoutMillis;

    private static String index;

    private static String type;

    private static int id;

    private static int shardNum;

    private static int replicaNum;

    private static int ConnectTimeout;

    private static int SocketTimeout;

    private static void initProperties() throws EsException
    {
        Properties properties = new Properties();
        String path = System.getProperty("user.dir") + File.separator + "conf" + File.separator
                + "es-example.properties";
        path = path.replace("\\", "\\\\");
        try
        {
            properties.load(new FileInputStream(new File(path)));
        }
        catch (Exception e)
        {
            throw new EsException("Failed to load properties file : " + path);
        }
        esClientIP = properties.getProperty("ServerIP");
        MaxRetryTimeoutMillis = Integer.valueOf(properties.getProperty("MaxRetryTimeoutMillis"));
        ConnectTimeout = Integer.valueOf(properties.getProperty("ConnectTimeout"));
        SocketTimeout = Integer.valueOf(properties.getProperty("SocketTimeout"));
        esClientIPPort = Integer.valueOf(properties.getProperty("ServerIPPort"));
        isSecureMode = properties.getProperty("isSecureMode");
        index = properties.getProperty("index");
        type = properties.getProperty("type");
        id = Integer.valueOf(properties.getProperty("id"));
        shardNum = Integer.valueOf(properties.getProperty("shardNum"));
        replicaNum = Integer.valueOf(properties.getProperty("replicaNum"));
        LOG.info("esClientIP:" + esClientIP);
        LOG.info("esClientIPPort:" + esClientIPPort);
        LOG.info("MaxRetryTimeoutMillis:" + MaxRetryTimeoutMillis);
        LOG.info("index:" + index);
        LOG.info("shardNum:" + shardNum);
        LOG.info("replicaNum:" + replicaNum);
        LOG.info("isSecureMode:" + isSecureMode);
        LOG.info("type:" + type);
        LOG.info("id:" + id);
    }

    /**
     * set configurations about authentication.
     * @throws EsException
     */
    private static void setSecConfig() throws EsException
    {
        String krb5ConfFile = RestClientTest.class.getClassLoader().getResource("krb5.conf").getFile();
        LOG.info("krb5ConfFile:" + krb5ConfFile);
        System.setProperty("java.security.krb5.conf", krb5ConfFile);

        String jaasPath = RestClientTest.class.getClassLoader().getResource("jaas.conf").getFile();
        LOG.info("jaasPath:" + jaasPath);
        System.setProperty("java.security.auth.login.config", jaasPath);
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");

    }

    /**
     * get one rest client instance.
     * @return
     * @throws EsException
     */
    private static RestClient getRestClient() throws EsException
    {
        RestClientBuilder builder = null;
        RestClient restClient = null;
        if (isSecureMode.equals("true"))
        { // 安全模式
            setSecConfig();
        }
        builder = RestClient.builder(new HttpHost(esClientIP, esClientIPPort, "http"));
        Header[] defaultHeaders = new Header[] { new BasicHeader("Accept", "application/json"),
                new BasicHeader("Content-type", "application/json") };

        builder = builder.setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback()
        {
            public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder)
            {
                return requestConfigBuilder.setConnectTimeout(ConnectTimeout).setSocketTimeout(SocketTimeout);
            }
        }).setMaxRetryTimeoutMillis(MaxRetryTimeoutMillis);

        builder.setDefaultHeaders(defaultHeaders);
        restClient = builder.build();
        LOG.info("The RestClient has been created !");
        return restClient;
    }

    /**
     * create one index with shard number.
     * @param restClient
     * @throws EsException
     * @throws IOException
     */
    private static void createIndexWithShardNum(RestClient restClient) throws EsException, IOException
    {
        Map<String, String> params = Collections.singletonMap("pretty", "true");
        String jsonString = "{" + "\"settings\":{" + "\"number_of_shards\":\"" + shardNum + "\","
                + "\"number_of_replicas\":\"" + replicaNum + "\"" + "}}";

        HttpEntity entity = new NStringEntity(jsonString, ContentType.APPLICATION_JSON);
        Response response = null;
        try
        {
            response = restClient.performRequest("PUT", "/" + index, params, entity);
        }
        catch (IOException e)
        {
            LOG.error("Failed to create collection", e);
            throw new EsException("Failed to create collection");
        }
        catch (Exception e)
        {
            LOG.error("Failed to create collection", e);
            throw new EsException("unknown exception");
        }
        if (!(response.getStatusLine().getStatusCode() < 0))
        {
            LOG.info("Success to create collection[{}]", index);
        }
        else
        {
            LOG.error("Failed to create collection[{}], cause : {}", index, response.getStatusLine().getReasonPhrase());
            throw new EsException("Failed to create collection");
        }
    }

    /**
     * query the cluster's health
     * @param restClient
     * @throws EsException
     */
    private static void queryEsClusterHealth(RestClient restClient) throws EsException
    {
        Response rsp = null;
        try
        {
            rsp = restClient.performRequest("GET", "/_cluster/health");
            Assert.assertEquals(rsp.getStatusLine().getStatusCode(), HttpStatus.SC_OK);
            LOG.info("response entity is : " + EntityUtils.toString(rsp.getEntity()));
        }
        catch (Exception e)
        {
            Assert.fail();
        }

    }

    /**
     * put one doc to the index
     * @param restClient
     * @throws Exception
     */
    private static void putData(RestClient restClient) throws Exception
    {
        String jsonString = "{" + "\"name\":\"Elasticsearch Reference\"," + "\"author\":\"Alex Yang \","
                + "\"pubinfo\":\"Beijing,China. \"," + "\"pubtime\":\"2016-07-16\","
                + "\"desc\":\"Elasticsearch is a highly scalable open-source full-text search and analytics engine.\""
                + "}";

        Map<String, String> params = Collections.singletonMap("pretty", "true");
        HttpEntity entity = new NStringEntity(jsonString, ContentType.APPLICATION_JSON);
        Response response = null;
        try
        {
            response = restClient.performRequest("PUT", "/" + index + "/" + type + "/" + id, params, entity);
        }
        catch (IOException e)
        {
            LOG.error("Failed to create collection", e);
            throw new EsException("Failed to create collection");
        }
        catch (Exception e)
        {
            LOG.error("Failed to create collection", e);
            throw new EsException("unknown exception");
        }
        if (!(response.getStatusLine().getStatusCode() == HttpStatus.SC_OK))
        {
            LOG.info("Success to create collection[{}]", index);
        }
        else
        {
            System.out.println("Failed to create collection:" + index);
            LOG.error("Failed to create collection[{}], cause : {}", index, response.getStatusLine().getReasonPhrase());
            throw new EsException("Failed to create collection");
        }
    }

    /**
     * query all datas of one index.
     * @param restClient
     * @throws Exception
     */
    private static void queryData(RestClient restClient) throws Exception
    {
        Response rsp = null;
        try
        {
            rsp = restClient.performRequest("GET", "/" + index + "/_search?q=*:*&pretty=true");
            Assert.assertEquals(rsp.getStatusLine().getStatusCode(), HttpStatus.SC_OK);
            LOG.info("response entity is : " + EntityUtils.toString(rsp.getEntity()));
        }
        catch (Exception e)
        {
            Assert.fail();
        }
    }

    /**
     * delete one index
     * @param restClient
     * @throws Exception
     */
    private static void delete(RestClient restClient) throws Exception
    {
        Response rsp = null;
        try
        {
            rsp = restClient.performRequest("DELETE", "/" + index + "?&pretty=true");
            Assert.assertEquals(rsp.getStatusLine().getStatusCode(), HttpStatus.SC_OK);
            LOG.info("response entity is : " + EntityUtils.toString(rsp.getEntity()));
        }
        catch (Exception e)
        {
            Assert.fail();
        }
    }

    public static void main(String[] args) throws Exception
    {
        LOG.info("start to do es test !");
        initProperties();
        RestClient restClient = null;
        try
        {
            restClient = getRestClient();
            queryEsClusterHealth(restClient);
            createIndexWithShardNum(restClient);
            putData(restClient);
            queryData(restClient);
            delete(restClient);
        }
        finally
        {
            try
            {
                if (restClient != null)
                {
                    restClient.close();
                }
            }
            catch (IOException e)
            {
                LOG.warn("Failed to close RestClient", e);
            }
        }

    }

}
