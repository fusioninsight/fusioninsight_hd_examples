package com.huawei.fusioninsight.elasticsearch.example.lowlevel.delete;

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

public class DeleteIndex {

    private static final Logger LOG = LoggerFactory.getLogger(DeleteIndex.class);

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
     * Delete one index
     */
    private static void deleteIndex(RestClient restClientTest, String index) {
        Response response;
        try {
            Request request = new Request("DELETE", "/" + index + "?&pretty=true");
            response = restClientTest.performRequest(request);
            if (HttpStatus.SC_OK == response.getStatusLine().getStatusCode()) {
                LOG.info("DeleteIndex successful.");
            } else {
                LOG.error("DeleteIndex failed.");
            }
            LOG.info("DeleteIndex response entity is : " + EntityUtils.toString(response.getEntity()));
        } catch (Exception e) {
            LOG.error("DeleteIndex failed, exception occurred.", e);
        }
    }

    public static void main(String[] args) {

        LOG.info("Start to do deleteIndex request!");

        HwRestClient hwRestClient = new HwRestClient();
        RestClient restClient = hwRestClient.getRestClient();
        try {
            putData(restClient, "huawei1", "type1", "1");
            deleteIndex(restClient, "huawei1");
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
