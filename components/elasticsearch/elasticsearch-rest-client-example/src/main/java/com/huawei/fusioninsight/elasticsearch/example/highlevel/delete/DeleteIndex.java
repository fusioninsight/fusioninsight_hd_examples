package com.huawei.fusioninsight.elasticsearch.example.highlevel.delete;

import java.io.IOException;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.hwclient.HwRestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteIndex {
    private static final Logger LOG = LoggerFactory.getLogger(DeleteIndex.class);

    /**
     * Create or update index by json
     */
    private static void indexByJson(RestHighLevelClient highLevelClient, String index, String type, String id) {
        try {
            IndexRequest indexRequest = new IndexRequest(index, type, id);
            String jsonString = "{" + "\"user\":\"kimchy1\"," + "\"age\":\"100\"," + "\"postDate\":\"2020-01-01\"," +
                "\"message\":\"trying out Elasticsearch\"," + "\"reason\":\"daily update\"," + "\"innerObject1\":\"Object1\"," +
                "\"innerObject2\":\"Object2\"," + "\"innerObject3\":\"Object3\"," + "\"uid\":\"11\"" + "}";
            indexRequest.source(jsonString, XContentType.JSON);
            IndexResponse indexResponse = highLevelClient.index(indexRequest, RequestOptions.DEFAULT);

            LOG.info("IndexByJson response is {}.", indexResponse.toString());
        } catch (Exception e) {
            LOG.error("IndexByJson is failed,exception occurred.", e);
        }
    }

    /**
     * Delete the index
     */
    private static void deleteIndex(RestHighLevelClient highLevelClient, String index) {
        try {
            DeleteIndexRequest request = new DeleteIndexRequest(index);
            AcknowledgedResponse delateResponse = highLevelClient.indices().delete(request, RequestOptions.DEFAULT);
            if (delateResponse.isAcknowledged()) {
                LOG.info("Delete index is successful");
            } else {
                LOG.info("Delete index is failed");
            }
        } catch (Exception e) {
            LOG.error("Delete index : {} is failed, exception occurred.", index, e);
        }
    }

    public static void main(String[] args) {

        LOG.info("Start to do deleteIndex request !");
        RestHighLevelClient highLevelClient = null;
        HwRestClient hwRestClient = new HwRestClient();
        try {
            highLevelClient = new RestHighLevelClient(hwRestClient.getRestClientBuilder());
            indexByJson(highLevelClient, "huawei", "type1", "1");
            deleteIndex(highLevelClient, "huawei");
        } finally {
            try {
                if (highLevelClient != null) {
                    highLevelClient.close();
                }
            } catch (IOException e) {
                LOG.error("Failed to close RestHighLevelClient.", e);
            }
        }
    }
}
