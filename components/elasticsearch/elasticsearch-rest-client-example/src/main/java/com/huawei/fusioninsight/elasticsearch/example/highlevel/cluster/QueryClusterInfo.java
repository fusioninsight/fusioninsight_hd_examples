package com.huawei.fusioninsight.elasticsearch.example.highlevel.cluster;

import java.io.IOException;

import org.elasticsearch.action.main.MainResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.hwclient.HwRestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryClusterInfo {
    private static final Logger LOG = LoggerFactory.getLogger(QueryClusterInfo.class);

    /**
     * Get cluster information
     */
    private static void queryClusterInfo(RestHighLevelClient highLevelClient) {

        try {
            MainResponse response = highLevelClient.info(RequestOptions.DEFAULT);
            ClusterName clusterName = response.getClusterName();
            LOG.info("ClusterName:[{}], clusterUuid:[{}], nodeName:[{}], version:[{}].",
                clusterName.value(),
                response.getClusterUuid(),
                response.getNodeName(),
                response.getVersion().toString());
        } catch (Exception e) {
            LOG.error("QueryClusterInfo is failed,exception occurred.", e);
        }
    }

    public static void main(String[] args) {
        LOG.info("Start to do queryClusterInfo test !");
        RestHighLevelClient highLevelClient = null;
        HwRestClient hwRestClient = new HwRestClient();
        try {
            highLevelClient = new RestHighLevelClient(hwRestClient.getRestClientBuilder());
            queryClusterInfo(highLevelClient);
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
