package com.huawei.fusioninsight.elasticsearch.example.lowlevel.index;

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

public class CreateIndex {

    private static final Logger LOG = LoggerFactory.getLogger(CreateIndex.class);

    /**
     * Create one index with customized shard number and replica number.
     */
    private static void createIndexWithShardNum(RestClient restClientTest, String index) {
        Response rsp;
        int shardNum = 3;
        int replicaNum = 1;
        String jsonString =
            "{" + "\"settings\":{" + "\"number_of_shards\":\"" + shardNum + "\"," + "\"number_of_replicas\":\"" + replicaNum + "\"" + "}}";

        HttpEntity entity = new NStringEntity(jsonString, ContentType.APPLICATION_JSON);
        try {

            Request request = new Request("PUT", "/" + index);
            request.addParameter("pretty", "true");
            request.setEntity(entity);
            rsp = restClientTest.performRequest(request);
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

    public static void main(String[] args) {

        LOG.info("Start to do createIndex request !");

        HwRestClient hwRestClient = new HwRestClient();
        RestClient restClient = hwRestClient.getRestClient();
        try {
            createIndexWithShardNum(restClient, "huawei1");
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
