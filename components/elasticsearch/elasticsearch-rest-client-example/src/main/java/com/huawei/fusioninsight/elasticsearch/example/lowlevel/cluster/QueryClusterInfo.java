package com.huawei.fusioninsight.elasticsearch.example.lowlevel.cluster;

import org.apache.http.HttpStatus;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.hwclient.HwRestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryClusterInfo {

    private static final Logger LOG = LoggerFactory.getLogger(QueryClusterInfo.class);

    /**
     * Query the cluster's information
     */
    private static void queryClusterInfo(RestClient restClientTest) {
        Response response;
        try {
            Request request = new Request("GET", "/_cluster/health");
            //对返回结果进行处理，展示方式更直观
            request.addParameter("pretty", "true");
            response = restClientTest.performRequest(request);
            if (HttpStatus.SC_OK == response.getStatusLine().getStatusCode()) {
                LOG.info("QueryClusterInfo successful.");
            } else {
                LOG.error("QueryClusterInfo failed.");
            }
            LOG.info("QueryClusterInfo response entity is : " + EntityUtils.toString(response.getEntity()));
        } catch (Exception e) {
            LOG.error("QueryClusterInfo failed, exception occurred.", e);
        }

    }

    public static void main(String[] args) {

        LOG.info("Start to do queryClusterInfo request !");
        HwRestClient hwRestClient = new HwRestClient();
        RestClient restClient = hwRestClient.getRestClient();
        try {
            queryClusterInfo(restClient);
        } catch (Exception e) {
            LOG.error("There are exceptions occurred.", e);
        } finally {
            if (restClient != null) {
                try {
                    restClient.close();
                    LOG.info("Close the client successful.");
                } catch (Exception e1) {
                    LOG.error("Close the client failed.", e1);
                }
            }
        }
    }
}
