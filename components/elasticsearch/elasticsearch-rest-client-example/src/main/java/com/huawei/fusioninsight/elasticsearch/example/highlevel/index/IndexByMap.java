package com.huawei.fusioninsight.elasticsearch.example.highlevel.index;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.hwclient.HwRestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexByMap {
    private static final Logger LOG = LoggerFactory.getLogger(IndexByMap.class);

    /**
     * Create or update index by map
     */
    private static void indexByMap(RestHighLevelClient highLevelClient, String index, String type, String id) {
        try {
            Map<String, Object> dataMap = new HashMap<>();
            dataMap.put("user", "kimchy2");
            dataMap.put("age", "200");
            dataMap.put("postDate", new Date());
            dataMap.put("message", "trying out Elasticsearch");
            dataMap.put("reason", "daily update");
            dataMap.put("innerObject1", "Object1");
            dataMap.put("innerObject2", "Object2");
            dataMap.put("innerObject3", "Object3");
            dataMap.put("uid", "22");
            IndexRequest indexRequest = new IndexRequest(index, type, id).source(dataMap);
            IndexResponse indexResponse = highLevelClient.index(indexRequest, RequestOptions.DEFAULT);

            LOG.info("IndexByMap response is {}.", indexResponse.toString());
        } catch (Exception e) {
            LOG.error("IndexByMap is failed,exception occurred.", e);
        }
    }

    public static void main(String[] args) {

        LOG.info("Start to do indexByMap request !");
        RestHighLevelClient highLevelClient = null;
        HwRestClient hwRestClient = new HwRestClient();
        try {
            highLevelClient = new RestHighLevelClient(hwRestClient.getRestClientBuilder());
            indexByMap(highLevelClient, "huawei", "type1", "1");
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
