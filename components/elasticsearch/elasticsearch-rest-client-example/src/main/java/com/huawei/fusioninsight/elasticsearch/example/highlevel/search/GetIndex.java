package com.huawei.fusioninsight.elasticsearch.example.highlevel.search;

import java.io.IOException;

import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.hwclient.HwRestClient;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GetIndex {
    private static final Logger LOG = LoggerFactory.getLogger(GetIndex.class);

    /**
     * Get index information
     */
    private static void getIndex(RestHighLevelClient highLevelClient, String index, String type, String id) {

        try {
            GetRequest getRequest = new GetRequest(index, type, id);
            String[] includes = new String[] {"message", "test*"};
            String[] excludes = Strings.EMPTY_ARRAY;
            FetchSourceContext fetchSourceContext = new FetchSourceContext(true, includes, excludes);
            getRequest.fetchSourceContext(fetchSourceContext);
            getRequest.storedFields("message");
            GetResponse getResponse = highLevelClient.get(getRequest, RequestOptions.DEFAULT);

            LOG.info("GetIndex response is {}", getResponse.toString());
        } catch (Exception e) {
            LOG.error("GetIndex is failed,exception occurred.", e);
        }
    }

    public static void main(String[] args) {

        LOG.info("Start to do getIndex request !");
        RestHighLevelClient highLevelClient = null;
        HwRestClient hwRestClient = new HwRestClient();
        try {
            highLevelClient = new RestHighLevelClient(hwRestClient.getRestClientBuilder());
            getIndex(highLevelClient, "huawei", "type1", "1");
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
