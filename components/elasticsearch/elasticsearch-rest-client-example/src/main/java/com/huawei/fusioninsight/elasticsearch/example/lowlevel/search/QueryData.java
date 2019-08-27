package com.huawei.fusioninsight.elasticsearch.example.lowlevel.search;

import java.util.HashMap;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.hwclient.HwRestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

public class QueryData {

    private static final Logger LOG = LoggerFactory.getLogger(QueryData.class);

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

    public static void main(String[] args) {

        LOG.info("Start to do queryData Request !");
        HwRestClient hwRestClient = new HwRestClient();
        RestClient restClient = hwRestClient.getRestClient();
        try {
            putData(restClient, "huawei1", "type1", "1");
            queryData(restClient, "huawei1", "type1", "1");
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


