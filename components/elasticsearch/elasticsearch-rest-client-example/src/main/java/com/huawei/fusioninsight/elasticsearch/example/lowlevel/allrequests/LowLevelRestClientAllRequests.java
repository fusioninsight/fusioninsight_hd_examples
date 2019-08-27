package com.huawei.fusioninsight.elasticsearch.example.lowlevel.allrequests;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.hwclient.HwRestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

public class LowLevelRestClientAllRequests {

    private static final Logger LOG = LoggerFactory.getLogger(LowLevelRestClientAllRequests.class);

    /**
     * Query the cluster's information
     */
    private static void queryClusterInfo(RestClient restClientTest) {
        Response response;
        try {
            Request request = new Request("GET", "/_cluster/health");
            request.addParameter("pretty", "true");
            response = restClientTest.performRequest(request);

            if (HttpStatus.SC_OK == response.getStatusLine().getStatusCode()) {
                LOG.info("QueryClusterInfo successful.");
            } else {
                LOG.error("QueryClusterInfo failed.");
            }
            LOG.info("QueryClusterInfo response entity is : " + EntityUtils.toString(response.getEntity()));
        } catch (Exception e) {
            LOG.error("QueryClusterInfo failed, exception occurred.", e);
        }
    }

    /**
     * Check the existence of the index or not.
     */
    private static boolean indexIsExist(RestClient restClientTest, String index) {
        Response response;
        try {
            Request request = new Request("HEAD", "/" + index);
            request.addParameter("pretty", "true");
            response = restClientTest.performRequest(request);
            if (HttpStatus.SC_OK == response.getStatusLine().getStatusCode()) {
                LOG.info("Check index successful,index is exist : " + index);
                return true;
            }
            if (HttpStatus.SC_NOT_FOUND == response.getStatusLine().getStatusCode()) {
                LOG.info("Index is not exist : " + index);
                return false;
            }

        } catch (Exception e) {
            LOG.error("Check index failed, exception occurred.", e);
        }
        return false;
    }

    /**
     * Create one index with customized shard number and replica number.
     */
    private static void createIndexWithShardNum(RestClient restClientTest, String index) {
        Response response;
        int shardNum = 3;
        int replicaNum = 1;
        String jsonString =
            "{" + "\"settings\":{" + "\"number_of_shards\":\"" + shardNum + "\"," + "\"number_of_replicas\":\"" + replicaNum + "\"" + "}}";

        HttpEntity entity = new NStringEntity(jsonString, ContentType.APPLICATION_JSON);
        try {
            Request request = new Request("PUT", "/" + index);
            request.addParameter("pretty", "true");
            request.setEntity(entity);
            response = restClientTest.performRequest(request);
            if (HttpStatus.SC_OK == response.getStatusLine().getStatusCode()) {
                LOG.info("CreateIndexWithShardNum successful.");
            } else {
                LOG.error("CreateIndexWithShardNum failed.");
            }
            LOG.info("CreateIndexWithShardNum response entity is : " + EntityUtils.toString(response.getEntity()));
        } catch (Exception e) {
            LOG.error("CreateIndexWithShardNum failed, exception occurred.", e);
        }

    }

    /**
     * Write one document into the index
     */
    private static void putData(RestClient restClientTest, String index, String type, String id) {

        Gson gson = new Gson();
        Map<String, Object> esMap = new HashMap<>();
        esMap.put("name", "Happy");
        esMap.put("author", "Alex Yang");
        esMap.put("pubinfo", "Beijing,China");
        esMap.put("pubtime", "2020-01-01");
        esMap.put("description", "Elasticsearch is a highly scalable open-source full-text search and analytics engine.");
        String jsonString = gson.toJson(esMap);
        Map<String, String> params = Collections.singletonMap("pretty", "true");
        HttpEntity entity = new NStringEntity(jsonString, ContentType.APPLICATION_JSON);
        Response response;

        try {
            Request request = new Request("POST", "/" + index + "/" + type + "/" + id);
            request.addParameter("pretty", "true");
            request.setEntity(entity);
            response = restClientTest.performRequest(request);
            if (HttpStatus.SC_OK == response.getStatusLine().getStatusCode() ||
                HttpStatus.SC_CREATED == response.getStatusLine().getStatusCode()) {
                LOG.info("PutData successful.");
            } else {
                LOG.error("PutData failed.");
            }
            LOG.info("PutData response entity is : " + EntityUtils.toString(response.getEntity()));
        } catch (Exception e) {
            LOG.error("PutData failed, exception occurred.", e);
        }

    }

    /**
     * Query all data of one index.
     */
    private static void queryData(RestClient restClientTest, String index, String type, String id) {
        Response response;
        try {
            Request request = new Request("GET", "/" + index + "/" + type + "/" + id);
            request.addParameter("pretty", "true");
            response = restClientTest.performRequest(request);
            if (HttpStatus.SC_OK == response.getStatusLine().getStatusCode()) {
                LOG.info("QueryData successful.");
            } else {
                LOG.error("QueryData failed.");
            }
            LOG.info("QueryData response entity is : " + EntityUtils.toString(response.getEntity()));
        } catch (Exception e) {
            LOG.error("QueryData failed, exception occurred.", e);
        }
    }

    /**
     * Delete one index
     */
    private static void deleteIndex(RestClient restClientTest, String index) {
        Response response;
        try {
            Request request = new Request("DELETE", "/" + index + "?&pretty=true");
            response = restClientTest.performRequest(request);
            if (HttpStatus.SC_OK == response.getStatusLine().getStatusCode()) {
                LOG.info("Delete index successful.");
            } else {
                LOG.error("Delete index failed.");
            }
            LOG.info("Delete index response entity is : " + EntityUtils.toString(response.getEntity()));
        } catch (Exception e) {
            LOG.error("Delete index failed, exception occurred.", e);
        }
    }

    /**
     * Delete some documents by query in one index
     */
    private static void deleteSomeDocumentsInIndex(RestClient restClientTest, String index, String field, String value) {
        String jsonString = "{\n" + "  \"query\": {\n" + "    \"match\": { \"" + field + "\":\"" + value + "\"}\n" + "  }\n" + "}";
        HttpEntity entity = new NStringEntity(jsonString, ContentType.APPLICATION_JSON);
        Response response;
        try {
            Request request = new Request("POST", "/" + index + "/_delete_by_query");
            request.addParameter("pretty", "true");
            request.setEntity(entity);
            response = restClientTest.performRequest(request);
            if (HttpStatus.SC_OK == response.getStatusLine().getStatusCode()) {
                LOG.info("DeleteSomeDocumentsInIndex successful.");
            } else {
                LOG.error("DeleteSomeDocumentsInIndex failed.");
            }
            LOG.info("DeleteSomeDocumentsInIndex response entity is : " + EntityUtils.toString(response.getEntity()));
        } catch (Exception e) {
            LOG.error("DeleteSomeDocumentsInIndex failed, exception occurred.", e);
        }
    }

    /**
     * Flush one index data to storage and clearing the internal transaction log
     */
    private static void flushOneIndex(RestClient restClientTest, String index) {
        Response flushresponse;
        try {
            Request request = new Request("POST", "/" + index + "/_flush");
            request.addParameter("pretty", "true");
            flushresponse = restClientTest.performRequest(request);
            LOG.info(EntityUtils.toString(flushresponse.getEntity()));

            if (HttpStatus.SC_OK == flushresponse.getStatusLine().getStatusCode()) {
                LOG.info("Flush successful.");
            } else {
                LOG.error("Flush failed.");
            }
            LOG.info("Flush response entity is : " + EntityUtils.toString(flushresponse.getEntity()));
        } catch (Exception e) {
            LOG.error("Flush failed, exception occurred.", e);
        }
    }

    /**
     * Send a bulk request
     */
    private static void bulk(RestClient restClientTest, String index, String type) {

        //需要写入的总文档数
        long totalRecordNum = 10;
        //一次bulk写入的文档数
        long oneCommit = 5;
        long circleNumber = totalRecordNum / oneCommit;
        StringEntity entity;
        Gson gson = new Gson();
        Map<String, Object> esMap = new HashMap<>();
        String str = "{ \"index\" : { \"_index\" : \"" + index + "\", \"_type\" :  \"" + type + "\"} }";

        for (int i = 1; i <= circleNumber; i++) {
            StringBuffer buffer = new StringBuffer();
            for (int j = 1; j <= oneCommit; j++) {
                esMap.clear();
                esMap.put("name", "Linda");
                esMap.put("age", ThreadLocalRandom.current().nextInt(18, 100));
                esMap.put("height", (float)ThreadLocalRandom.current().nextInt(140, 220));
                esMap.put("weight", (float)ThreadLocalRandom.current().nextInt(70, 200));
                esMap.put("cur_time", System.currentTimeMillis());

                String strJson = gson.toJson(esMap);
                buffer.append(str).append("\n");
                buffer.append(strJson).append("\n");
            }
            entity = new StringEntity(buffer.toString(), ContentType.APPLICATION_JSON);
            entity.setContentEncoding("UTF-8");
            Response response;
            try {
                Request request = new Request("PUT", "/_bulk");
                request.setEntity(entity);
                request.addParameter("pretty", "true");
                response = restClientTest.performRequest(request);
                if (HttpStatus.SC_OK == response.getStatusLine().getStatusCode()) {
                    LOG.info("Already input documents : " + oneCommit * i);
                } else {
                    LOG.error("Bulk failed.");
                }
                LOG.info("Bulk response entity is : " + EntityUtils.toString(response.getEntity()));
            } catch (Exception e) {
                LOG.error("Bulk failed, exception occurred.", e);
            }
        }
    }

    public static void main(String[] args) {

        LOG.info("Start to do low level rest client request !");
        HwRestClient hwRestClient = new HwRestClient();
        RestClient restClient = hwRestClient.getRestClient();
        try {
            queryClusterInfo(restClient);
            if (indexIsExist(restClient, "huawei")) {
                deleteIndex(restClient, "huawei");
            }
            createIndexWithShardNum(restClient, "huawei");
            putData(restClient, "huawei", "type1", "1");
            bulk(restClient, "huawei", "type1");
            queryData(restClient, "huawei", "type1", "1");
            deleteSomeDocumentsInIndex(restClient, "huawei", "pubinfo", "Beijing");
            flushOneIndex(restClient, "huawei");
            deleteIndex(restClient, "huawei");
        } catch (Exception e) {
            LOG.error("There are exceptions occurred.", e);
        } finally {
            if (restClient != null) {
                try {
                    restClient.close();
                    LOG.info("Close the client successful.");
                } catch (Exception e1) {
                    LOG.error("Close the client failed.", e1);
                }
            }
        }
    }
}


