package com.huawei.fusioninsight.elasticsearch.example.highlevel.index;

import java.io.IOException;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.hwclient.HwRestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexByXContentBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(IndexByXContentBuilder.class);

    /**
     * Create or update index by XContentBuilder
     */
    private static void indexByXContentBuilder(RestHighLevelClient highLevelClient, String index, String type, String id) {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            {
                builder.field("user", "kimchy3");
                builder.field("age", "300");
                builder.field("postDate", "2020-01-01");
                builder.field("message", "trying out Elasticsearch");
                builder.field("reason", "daily update");
                builder.field("innerObject1", "Object1");
                builder.field("innerObject2", "Object2");
                builder.field("innerObject3", "Object3");
                builder.field("uid", "33");

            }
            builder.endObject();
            IndexRequest indexRequest = new IndexRequest(index, type, id).source(builder);
            IndexResponse indexResponse = highLevelClient.index(indexRequest, RequestOptions.DEFAULT);

            LOG.info("IndexByXContentBuilder response is {}", indexResponse.toString());
        } catch (Exception e) {
            LOG.error("IndexByXContentBuilder is failed,exception occurred.", e);
        }
    }

    public static void main(String[] args) {

        LOG.info("Start to do indexByXContentBuilder request !");
        RestHighLevelClient highLevelClient = null;
        HwRestClient hwRestClient = new HwRestClient();
        try {
            highLevelClient = new RestHighLevelClient(hwRestClient.getRestClientBuilder());
            indexByXContentBuilder(highLevelClient, "huawei", "type1", "1");
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
