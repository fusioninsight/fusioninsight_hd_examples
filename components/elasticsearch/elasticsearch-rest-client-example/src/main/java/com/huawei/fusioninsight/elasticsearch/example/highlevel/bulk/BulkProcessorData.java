package com.huawei.fusioninsight.elasticsearch.example.highlevel.bulk;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.hwclient.HwRestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * BulkProcessor简化Bulk API的使用，并且使整个批量操作透明化。
 */
public class BulkProcessorData {
    private static final Logger LOG = LoggerFactory.getLogger(BulkProcessorData.class);

    private static RestHighLevelClient highLevelClient;

    private static BulkProcessor bulkProcessor = null;

    //数据条数达到1000时进行刷新操作
    private int bulkNum = 1000;

    //数据量大小达到5M进行刷新操作
    private int bulkSize = 5;

    //设置允许执行的并发请求数
    private int concurrentRequestsNum = 5;

    //设置刷新间隔时间，如果超过刷新时间则BulkRequest挂起
    private int flushTime = 10;

    //后退策略,最大重试次数
    private int maxRerty = 3;

    //批量写入的总条数
    private long totalNum = 10000;

    /**
     * init BulkProcessor
     */
    private void init() {

        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest bulkRequest) {
                int numberOfActions = bulkRequest.numberOfActions();
                LOG.info("Executing bulk {} with {} requests", executionId, numberOfActions);
            }

            @Override
            public void afterBulk(long executionId, BulkRequest bulkRequest, BulkResponse bulkResponse) {
                if (bulkResponse.hasFailures()) {
                    LOG.warn("Bulk {} executed with failures", executionId);
                } else {
                    LOG.info("Bulk {} completed in {} milliseconds", executionId, bulkResponse.getTook().getMillis());
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest bulkRequest, Throwable throwable) {
                LOG.error("Failed to execute bulk", throwable);
            }
        };

        BiConsumer<BulkRequest, ActionListener<BulkResponse>> bulkConsumer =
                (request, bulkListener) -> highLevelClient.bulkAsync(request, RequestOptions.DEFAULT, bulkListener);
        bulkProcessor = BulkProcessor.builder(bulkConsumer, listener)
                .setBulkActions(bulkNum)
                .setBulkSize(new ByteSizeValue(bulkSize, ByteSizeUnit.MB))
                .setConcurrentRequests(concurrentRequestsNum)
                .setFlushInterval(TimeValue.timeValueSeconds(flushTime))
                .setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(1L), maxRerty))
                .build();
    }

    private void destroy() {
        try {
            //执行关闭方法会把bulk剩余的数据都写入ES再执行关闭
            bulkProcessor.awaitClose(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOG.error("Failed to close bulkProcessor", e);
        }
        LOG.info("bulkProcessor closed.");
    }

    private void inputData(String index, String type) {
        Map<String, Object> jsonMap = new HashMap<>();
        for (int i = 1; i <= totalNum; i++) {
            jsonMap.clear();
            jsonMap.put("user", "Linda");
            jsonMap.put("age", ThreadLocalRandom.current().nextInt(18, 100));
            jsonMap.put("postDate", "2020-01-01");
            jsonMap.put("height", (float) ThreadLocalRandom.current().nextInt(140, 220));
            jsonMap.put("weight", (float) ThreadLocalRandom.current().nextInt(70, 200));
            bulkProcessor.add(new IndexRequest(index, type).source(jsonMap));
        }
    }

    public static void main(String[] args) {
        LOG.info("Start to do bulk request.");
        HwRestClient hwRestClient = new HwRestClient();
        BulkProcessorData bulkProcessorData = new BulkProcessorData();
        try {
            highLevelClient = new RestHighLevelClient(hwRestClient.getRestClientBuilder());
            bulkProcessorData.init();
            bulkProcessorData.inputData("indexname", "type");
        } finally {
            try {
                if (bulkProcessor != null) {
                    bulkProcessorData.destroy();
                }
                if (highLevelClient != null) {
                    highLevelClient.close();
                }
            } catch (IOException e) {
                LOG.error("Failed to close RestHighLevelClient.", e);
            }
        }
    }
}
