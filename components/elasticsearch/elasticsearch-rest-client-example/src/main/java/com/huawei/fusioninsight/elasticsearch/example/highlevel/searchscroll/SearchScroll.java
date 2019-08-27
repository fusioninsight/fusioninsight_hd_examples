package com.huawei.fusioninsight.elasticsearch.example.highlevel.searchscroll;

import java.io.IOException;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.hwclient.HwRestClient;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.elasticsearch.index.query.QueryBuilders.matchQuery;

public class SearchScroll {
    private static final Logger LOG = LoggerFactory.getLogger(SearchScroll.class);

    /**
     * Send a search scroll request
     */
    private static String searchScroll(RestHighLevelClient highLevelClient, String index) {
        String scrollId;
        try {
            final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
            SearchRequest searchRequest = new SearchRequest(index);
            searchRequest.scroll(scroll);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(matchQuery("title", "Elasticsearch"));
            searchRequest.source(searchSourceBuilder);

            SearchResponse searchResponse = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            scrollId = searchResponse.getScrollId();
            SearchHit[] searchHits = searchResponse.getHits().getHits();
            LOG.info("SearchHits is {}", searchResponse.toString());

            while (searchHits != null && searchHits.length > 0) {
                SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
                scrollRequest.scroll(scroll);
                searchResponse = highLevelClient.scroll(scrollRequest, RequestOptions.DEFAULT);
                scrollId = searchResponse.getScrollId();
                searchHits = searchResponse.getHits().getHits();
                LOG.info("SearchHits is {}", searchResponse.toString());
            }
        } catch (Exception e) {
            LOG.error("SearchScroll is failed,exception occured.", e);
            return null;
        }
        return scrollId;
    }

    /**
     * Create or update index by json
     */
    private static void indexByJson(RestHighLevelClient highLevelClient, String index, String type, String id) {
        try {
            IndexRequest indexRequest = new IndexRequest(index, type, id);
            String jsonString = "{" + "\"user\":\"kimchy1\"," + "\"age\":\"100\"," + "\"postDate\":\"2020-01-01\"," +
                "\"message\":\"trying out Elasticsearch\"," + "\"reason\":\"daily update\"," + "\"innerObject1\":\"Object1\"," +
                "\"innerObject2\":\"Object2\"," + "\"innerObject3\":\"Object3\"," + "\"uid\":\"11\"" + "}";
            indexRequest.source(jsonString, XContentType.JSON);
            IndexResponse indexResponse = highLevelClient.index(indexRequest, RequestOptions.DEFAULT);

            LOG.info("IndexByJson response is {}.", indexResponse.toString());
        } catch (Exception e) {
            LOG.error("IndexByJson is failed,exception occurred.", e);
        }
    }

    /**
     * Clear a search scroll
     */
    private static void clearScroll(RestHighLevelClient highLevelClient, String scrollId) {

        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        ClearScrollResponse clearScrollResponse;
        try {
            clearScrollResponse = highLevelClient.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
            if (clearScrollResponse.isSucceeded()) {
                LOG.info("ClearScroll is successful.");
            } else {
                LOG.error("ClearScroll is failed.");
            }
        } catch (IOException e) {
            LOG.error("ClearScroll is failed,exception occured.", e);
        }
    }

    public static void main(String[] args) {

        LOG.info("Start to do searchScroll request !");
        RestHighLevelClient highLevelClient = null;
        HwRestClient hwRestClient = new HwRestClient();
        try {
            highLevelClient = new RestHighLevelClient(hwRestClient.getRestClientBuilder());
            indexByJson(highLevelClient, "huawei", "type1", "1");
            String scroollId = searchScroll(highLevelClient, "huawei");
            clearScroll(highLevelClient, scroollId);
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
