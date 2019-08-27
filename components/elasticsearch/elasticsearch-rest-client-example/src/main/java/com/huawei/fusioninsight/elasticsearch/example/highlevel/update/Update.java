package com.huawei.fusioninsight.elasticsearch.example.highlevel.update;

import java.io.IOException;
import java.util.Date;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.hwclient.HwRestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Update {
    private static final Logger LOG = LoggerFactory.getLogger(Update.class);

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
     * Update index
     */
    private static void update(RestHighLevelClient highLevelClient, String index, String type, String id) {
        try {

            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            {   // update information
                builder.field("postDate", new Date());
                builder.field("reason", "update again");
            }
            builder.endObject();
            UpdateRequest request = new UpdateRequest(index, type, id).doc(builder);
            UpdateResponse updateResponse = highLevelClient.update(request, RequestOptions.DEFAULT);

            LOG.info("Update response is {}.", updateResponse.toString());
        } catch (Exception e) {
            LOG.error("Update is failed,exception occurred.", e);
        }
    }

    public static void main(String[] args) {

        LOG.info("Start to do update request !");
        RestHighLevelClient highLevelClient = null;
        HwRestClient hwRestClient = new HwRestClient();
        try {
            highLevelClient = new RestHighLevelClient(hwRestClient.getRestClientBuilder());
            indexByJson(highLevelClient, "huawei", "type1", "1");
            update(highLevelClient, "huawei", "type1", "1");
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
