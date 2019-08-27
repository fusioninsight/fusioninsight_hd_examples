package com.huawei.fusioninsight.elasticsearch.example.highlevel.multithread;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.hwclient.HwRestClient;
import org.elasticsearch.rest.RestStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultithreadRequest {
    private static final Logger LOG = LoggerFactory.getLogger(MultithreadRequest.class);

    /**
     * Bulk request can be used to to execute multiple index,update or delete
     * operations using a single request.
     */
    private static void bulk(RestHighLevelClient highLevelClient, String index, String type) {
        try {
            BulkRequest request = new BulkRequest();
            BulkResponse bulkResponse;
            Map<String, Object> jsonMap = new HashMap<>();
            //需要写入的总文档数
            long totalRecordNum = 10000;
            //一次bulk写入的文档数,推荐一次写入的大小为5M-15M
            long oneCommit = 1000;
            long circleNumber = totalRecordNum / oneCommit;
            for (int i = 1; i <= circleNumber; i++) {
                jsonMap.clear();
                for (int j = 0; j < oneCommit; j++) {
                    jsonMap.put("user", "Linda");
                    jsonMap.put("age", ThreadLocalRandom.current().nextInt(18, 100));
                    jsonMap.put("postDate", "2020-01-01");
                    jsonMap.put("height", (float) ThreadLocalRandom.current().nextInt(140, 220));
                    jsonMap.put("weight", (float) ThreadLocalRandom.current().nextInt(70, 200));
                    jsonMap.put("uid", i);
                }
                request.add(new IndexRequest(index, type).source(jsonMap));
                bulkResponse = highLevelClient.bulk(request, RequestOptions.DEFAULT);
                if (RestStatus.OK.equals((bulkResponse.status()))) {
                  //bulk请求中如果有失败的条目，请在业务代码中进行重试处理
                    if (bulkResponse.hasFailures()) {
                        LOG.warn("There are some failures in bulk response,please retry in client.");
                    } else {
                        LOG.info("Bulk successful");
                    }
                } else {
                    LOG.warn("Bulk failed.");
                }
            }
        } catch (Exception e) {
            LOG.error("Bulk is failed,exception occurred.", e);
        }
    }

    /**
     * The thread that send a bulk request
     */
    public static class sendRequestThread implements Runnable {

        private RestHighLevelClient highLevelClientTh;

        @Override
        public void run() {
            LOG.info("Thread begin.");
            HwRestClient hwRestClient = new HwRestClient();
            try {
                highLevelClientTh = new RestHighLevelClient(hwRestClient.getRestClientBuilder());
                LOG.info("Thread name: " + Thread.currentThread().getName());
                bulk(highLevelClientTh, "huawei", "type1");
            } catch (Exception e) {
                LOG.error("SendRequestThread had exception.", e);
            } finally {
                if (highLevelClientTh != null) {
                    try {
                        highLevelClientTh.close();
                        LOG.info("Close the highLevelClient successful in thread : " + Thread.currentThread().getName());
                    } catch (IOException e) {
                        LOG.error("Close the highLevelClient failed.", e);
                    }
                }
            }
        }
    }

    public static void main(String[] args) {
        LOG.info("Start to do bulk request !");
        ExecutorService fixedThreadPool;
        try {
            int threadPoolSize = 2;
            int jobNumber = 5;
            fixedThreadPool = Executors.newFixedThreadPool(threadPoolSize);
            for (int i = 0; i < jobNumber; i++) {
                fixedThreadPool.execute(new sendRequestThread());
            }
            fixedThreadPool.shutdown();
        } catch (Exception e) {
            LOG.error("SendRequestThread is failed,exception occured.", e);
        }
    }
}
