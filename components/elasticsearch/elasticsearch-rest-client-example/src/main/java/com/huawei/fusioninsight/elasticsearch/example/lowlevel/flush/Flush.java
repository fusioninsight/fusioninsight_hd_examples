package com.huawei.fusioninsight.elasticsearch.example.lowlevel.flush;

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

public class Flush {
    private static final Logger LOG = LoggerFactory.getLogger(Flush.class);

    /**
     * Write one document into the index
     */
    private static void putData(RestClient restClientTest, String index, String type, String id) {
        Gson gson = new Gson();
        Map<String, Object> esMap = new HashMap<String, Object>();
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
     * Flush one index data to storage and clearing the internal transaction log
     */
    private static void flushOneIndex(RestClient restClientTest, String index) {
        Response flushRsp;
        try {
            Request request = new Request("POST", "/" + index + "/_flush");
            request.addParameter("pretty", "true");
            flushRsp = restClientTest.performRequest(request);
            LOG.info(EntityUtils.toString(flushRsp.getEntity()));

            if (HttpStatus.SC_OK == flushRsp.getStatusLine().getStatusCode()) {
                LOG.info("Flush one index successful.");
            } else {
                LOG.error("Flush one index failed.");
            }
            LOG.info("Flush one index response entity is : " + EntityUtils.toString(flushRsp.getEntity()));
        } catch (Exception e) {
            LOG.error("Flush one index failed, exception occurred.", e);
        }
    }

    /**
     * Flush all indexes data to storage and clearing the internal transaction log
     * The user who performs this operation must belong to the "supergroup" group.
     */
    private static void flushAllIndexes(RestClient restClientTest) {
        Response flushRsp;
        try {
            Request request = new Request("POST", "/_all/_flush");
            request.addParameter("pretty", "true");
            flushRsp = restClientTest.performRequest(request);
            if (HttpStatus.SC_OK == flushRsp.getStatusLine().getStatusCode()) {
                LOG.info("Flush all indexes successful.");
            } else {
                LOG.error("Flush all indexes failed.");
            }
            LOG.info("Flush all indexes response entity is : " + EntityUtils.toString(flushRsp.getEntity()));
        } catch (Exception e) {
            LOG.error("Flush all indexes failed, exception occurred.", e);
        }
    }

    public static void main(String[] args) {

        LOG.info("Start to do flush request!");

        HwRestClient hwRestClient = new HwRestClient();
        RestClient restClient = hwRestClient.getRestClient();
        try {
            putData(restClient, "huawei1", "type1", "1");
            flushOneIndex(restClient, "huawei1");
            flushAllIndexes(restClient);
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