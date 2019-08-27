package com.huawei.fusioninsight.elasticsearch.example.highlevel.bulk;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.hwclient.HwRestClient;
import org.elasticsearch.rest.RestStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Bulk {
    private static final Logger LOG = LoggerFactory.getLogger(Bulk.class);

    /**
     * Bulk request can be used to to execute multiple index,update or delete
     * operations using a single request.
     */
    private static void bulk(RestHighLevelClient highLevelClient, String index, String type) {

        try {
            Map<String, Object> jsonMap = new HashMap<>();
            for (int i = 1; i <= 100; i++) {
                BulkRequest request = new BulkRequest();
                for (int j = 1; j <= 1000; j++) {
                    jsonMap.clear();
                    jsonMap.put("user", "Linda");
                    jsonMap.put("age", ThreadLocalRandom.current().nextInt(18, 100));
                    jsonMap.put("postDate", "2020-01-01");
                    jsonMap.put("height", (float) ThreadLocalRandom.current().nextInt(140, 220));
                    jsonMap.put("weight", (float) ThreadLocalRandom.current().nextInt(70, 200));
                    request.add(new IndexRequest(index, type).source(jsonMap));
                }
                BulkResponse bulkResponse = highLevelClient.bulk(request, RequestOptions.DEFAULT);

                if (RestStatus.OK.equals((bulkResponse.status()))) {
                    LOG.info("Bulk is successful");
                } else {
                    LOG.info("Bulk is failed");
                }
            }
        } catch (Exception e) {
            LOG.error("Bulk is failed,exception occurred.", e);
        }
    }

    public static void main(String[] args) {
        LOG.info("Start to do bulk request.");
        RestHighLevelClient highLevelClient = null;
        HwRestClient hwRestClient = new HwRestClient();
        try {
            highLevelClient = new RestHighLevelClient(hwRestClient.getRestClientBuilder());
            bulk(highLevelClient, "indexname", "type1");
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
