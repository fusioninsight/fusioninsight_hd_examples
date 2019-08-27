package com.huawei.fusioninsight.elasticsearch.example.highlevel.search;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.hwclient.HwRestClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Search {
    private static final Logger LOG = LoggerFactory.getLogger(Search.class);

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
     * Search some information in index
     */
    private static void search(RestHighLevelClient highLevelClient, String index) {
        try {
            SearchRequest searchRequest = new SearchRequest(index);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.termQuery("user", "kimchy1"));

            searchSourceBuilder.sort(new FieldSortBuilder("_doc").order(SortOrder.ASC));

            // Source filter
            String[] includeFields = new String[] {"message", "user", "innerObject*"};
            String[] excludeFields = new String[] {"postDate"};
            searchSourceBuilder.fetchSource(includeFields, excludeFields);
            // Control which fields get included or
            // excluded
            // Request Highlighting
            HighlightBuilder highlightBuilder = new HighlightBuilder();
            HighlightBuilder.Field highlightUser = new HighlightBuilder.Field("user");
            // Create a field highlighter for
            // the user field
            highlightBuilder.field(highlightUser);
            searchSourceBuilder.highlighter(highlightBuilder);
            searchSourceBuilder.from(0);
            searchSourceBuilder.size(2);
            searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
            searchRequest.source(searchSourceBuilder);
            SearchResponse searchResponse = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);

            LOG.info("Search response is {}.", searchResponse.toString());
        } catch (Exception e) {
            LOG.error("Search is failed,exception occurred.", e);
        }
    }

    public static void main(String[] args) {

        LOG.info("Start to do search request !");
        RestHighLevelClient highLevelClient = null;
        HwRestClient hwRestClient = new HwRestClient();
        try {
            highLevelClient = new RestHighLevelClient(hwRestClient.getRestClientBuilder());
            indexByJson(highLevelClient, "huawei", "type1", "1");
            search(highLevelClient, "huawei");
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
