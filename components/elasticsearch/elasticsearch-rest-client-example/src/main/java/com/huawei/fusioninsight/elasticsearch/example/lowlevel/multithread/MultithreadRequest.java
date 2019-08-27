package com.huawei.fusioninsight.elasticsearch.example.lowlevel.multithread;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.http.HttpStatus;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.hwclient.HwRestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

public class MultithreadRequest {

    private static final Logger LOG = LoggerFactory.getLogger(MultithreadRequest.class);

    private static final String INDEX_SETTING_KEY = "index";

    /**
     * Send a bulk request
     */
    private static void bulk(RestClient restClientTest, String index, String type) {
        StringEntity entity;
        Gson gson = new Gson();
        //需要写入的总文档数
        long totalRecordNum = 10000;
        //一次bulk写入的文档数,推荐一次写入的大小为5M-15M
        long oneCommit = 1000;
        long circleNumber = totalRecordNum / oneCommit;
        Map<String, Object> esMap = new HashMap<>();
        String str = "{ \"index\" : { \"_index\" : \"" + index + "\", \"_type\" :  \"" + type + "\"} }";
        for (int i = 1; i <= circleNumber; i++) {
            StringBuffer buffer = new StringBuffer();
            for (int j = 1; j <= oneCommit; j++) {
                esMap.clear();
                esMap.put("name", "Linda");
                esMap.put("age", ThreadLocalRandom.current().nextInt(18, 100));
                esMap.put("height", (float) ThreadLocalRandom.current().nextInt(140, 220));
                esMap.put("weight", (float) ThreadLocalRandom.current().nextInt(70, 200));
                esMap.put("cur_time", System.currentTimeMillis());
                String esStr = gson.toJson(esMap);
                buffer.append(str).append("\n");
                buffer.append(esStr).append("\n");
            }
            entity = new StringEntity(buffer.toString(), ContentType.APPLICATION_JSON);
            entity.setContentEncoding("UTF-8");
            Response rsp;
            try {
                Request request = new Request("PUT", "/_bulk");
                request.addParameter("pretty", "true");
                request.setEntity(entity);
                rsp = restClientTest.performRequest(request);
                if (HttpStatus.SC_OK == rsp.getStatusLine().getStatusCode()) {
                  //bulk请求中如果有失败的条目，请在业务代码中进行重试处理
                    boolean responseAllSuccess = parseResponse(EntityUtils.toString(rsp.getEntity()));
                    if (!responseAllSuccess) {
                        LOG.warn("There are some error in bulk response,please retry in client.");
                    } else {
                        LOG.info("Bulk successful");
                    }
                } else {
                    LOG.warn("Bulk failed.");
                }
            } catch (Exception e) {
                LOG.error("Bulk failed, exception occurred.", e);
            }
        }
    }

    /**
     * parse response
     *
     * @param content httpclient response
     * @return
     */
    private static boolean parseResponse(String content) {
        try {
            JsonObject object = new JsonParser().parse(content).getAsJsonObject();
            JsonArray array = object.getAsJsonArray("items");
            long successNum = 0L;
            long failedNum = 0L;
            Integer status;
            for (int i = 0; i < array.size(); i++) {
                JsonObject obj = (JsonObject) ((JsonObject) array.get(i)).get(INDEX_SETTING_KEY);
                status = obj.get("status").getAsInt();
                if (HttpStatus.SC_OK != status && HttpStatus.SC_CREATED != status) {
                    LOG.debug("Error response is {}", obj.toString());
                    failedNum++;
                } else {
                    successNum++;
                }
            }
            if (failedNum > 0) {
                LOG.info("Response has error,successNum is {},failedNum is {}.", successNum, failedNum);
                return false;
            } else {
                LOG.info("Response has no error.");
                return true;
            }
        } catch (Exception e) {
            LOG.error("parseResponse failed.", e);
            return false;
        }
    }

    public static class sendRequestThread implements Runnable {
        private RestClient restCientTh;

        private HwRestClient hwRestClient;

        @Override
        public void run() {
            LOG.info("Thread begin.");
            try {
                hwRestClient = new HwRestClient();
                restCientTh = hwRestClient.getRestClient();
                LOG.info("Thread name: " + Thread.currentThread().getName());
                bulk(restCientTh, "huawei1", "type1");

            } catch (Exception e) {
                LOG.error("SendRequestThread has exception.", e);
            } finally {
                if (restCientTh != null) {
                    try {
                        restCientTh.close();
                        LOG.info("Close the client successful in thread : " + Thread.currentThread().getName());
                    } catch (IOException e) {
                        LOG.error("Close the cient failed.", e);
                    }
                }
            }
        }
    }

    public static void main(String[] args) {
        LOG.info("Start to do multithread request !");
        ExecutorService fixedThreadPool;
        HwRestClient hwRestClient = new HwRestClient();
        RestClient restCientThNew;
        restCientThNew = hwRestClient.getRestClient();
        int threadPoolSize = 2;
        int jobNumber = 5;
        try {
            fixedThreadPool = Executors.newFixedThreadPool(threadPoolSize);
            for (int i = 0; i < jobNumber; i++) {
                fixedThreadPool.execute(new sendRequestThread());
            }
            fixedThreadPool.shutdown();
        } catch (Exception e) {
            LOG.error("There are exceptions in sendRequestThread.", e);
        } finally {
            if (restCientThNew != null) {
                try {
                    restCientThNew.close();
                    LOG.info("Close the client successful.");
                } catch (Exception e1) {
                    LOG.error("Close the client failed.", e1);
                }
            }
        }
    }
}