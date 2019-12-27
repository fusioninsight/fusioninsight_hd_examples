package com.huawei.fusioninsight.elasticsearch.example.lowlevel.exist;

import org.apache.http.HttpStatus;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.hwclient.HwRestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexIsExist {

    private static final Logger LOG = LoggerFactory.getLogger(IndexIsExist.class);

    /**
     * Check the existence of the index or not.
     */
    private static void indexIsExist(RestClient restClientTest, String index) {
        Response response;
        try {
            Request request = new Request("HEAD", "/" + index);
            response = restClientTest.performRequest(request);
            if (HttpStatus.SC_OK == response.getStatusLine().getStatusCode()) {
                LOG.info("***** Check index successful,index is exist : " + index + " *****");
            }
            if (HttpStatus.SC_NOT_FOUND == response.getStatusLine().getStatusCode()) {
                LOG.info("***** Index is not exist : " + index + " *****");
            }
        } catch (Exception e) {
            LOG.error("Check index failed, exception occurred.", e);
        }
    }

    public static void main(String[] args) {

        LOG.info("Start to do indexIsExist request !");
        HwRestClient hwRestClient = new HwRestClient();
        RestClient restClient = hwRestClient.getRestClient();
        try {
            indexIsExist(restClient, "huawei2");
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
