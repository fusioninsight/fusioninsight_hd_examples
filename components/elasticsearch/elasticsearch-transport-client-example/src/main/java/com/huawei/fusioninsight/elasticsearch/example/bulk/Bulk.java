package com.huawei.fusioninsight.elasticsearch.example.bulk;

import com.huawei.fusioninsight.elasticsearch.example.LoadProperties;
import com.huawei.fusioninsight.elasticsearch.transport.client.ClientFactory;
import com.huawei.fusioninsight.elasticsearch.transport.client.PreBuiltHWTransportClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;

import java.util.HashMap;
import java.util.Map;

public class Bulk {

    private static final Logger LOG = LogManager.getLogger(Bulk.class);
    private static long threadCommitNum;
    private static long processRecordNum = 10000;
    private static long BulkNum = 1000;
    private static int threadNum = 10;
    private static PreBuiltHWTransportClient client;

    /**
     * put data
     *
     * @param recordNum
     * @throws Exception
     */
    private static void DataInput(long recordNum, String index, String type) {
        long circleCommit = recordNum / BulkNum;
        Map<String, Object> esJson = new HashMap<String, Object>();

        for (int j = 0; j < circleCommit; j++) {
            long starttime = System.currentTimeMillis();
            BulkRequestBuilder bulkRequest = client.prepare().prepareBulk();
            for (int i = 0; i < BulkNum; i++) {
                esJson.clear();
                esJson.put("id", "1");
                esJson.put("name", "Linda");
                esJson.put("sex", "man");
                esJson.put("age", 78);
                esJson.put("height", 210);
                esJson.put("weight", 180);
                bulkRequest.add(client.prepare().prepareIndex(index, type).setSource(esJson));
            }
            BulkResponse bulkResponse = bulkRequest.get();
            if (bulkResponse.hasFailures()) {
                LOG.info("Batch indexing fail.");
            } else {
                LOG.info("Batch indexing success and put data time is " + (System.currentTimeMillis() - starttime));
            }

        }
    }

    private static void startThreadInput(int threadNum) {
        Thread[] th = new Thread[threadNum];
        multipleThInputRun[] hr_write = new multipleThInputRun[threadNum];
        for (int i = 0; i < threadNum; i++) {
            hr_write[i] = new multipleThInputRun();
        }
        for (int i = 0; i < threadNum; i++) {
            th[i] = new Thread(hr_write[i]);
        }
        for (int i = 0; i < threadNum; i++) {
            th[i].start();
        }
        for (int i = 0; i < threadNum; i++) {
            try {
                th[i].join();
            } catch (InterruptedException e) {
                LOG.debug("InterruptedException is ", e);
            }
        }
    }

    public static void main(String[] args) {
        threadCommitNum = processRecordNum / threadNum;
        try {
            ClientFactory.initConfiguration(LoadProperties.loadProperties());
            client = ClientFactory.getClient();
            startThreadInput(threadNum);
        } catch (Exception e) {
            LOG.error("Exception is", e);
        } finally {
            if (client != null) {
                try {
                    client.close();
                    LOG.info("Close the client successful in main.");
                } catch (Exception e1) {
                    LOG.error("Close the client failed in main.", e1);
                }
            }
        }
    }

    static class multipleThInputRun implements Runnable {
        public void run() {
            try {
                Bulk.DataInput(Bulk.threadCommitNum, "indexname", "type");
            } catch (Exception e) {
                LOG.error("Exception is", e);
            }
        }
    }
}
