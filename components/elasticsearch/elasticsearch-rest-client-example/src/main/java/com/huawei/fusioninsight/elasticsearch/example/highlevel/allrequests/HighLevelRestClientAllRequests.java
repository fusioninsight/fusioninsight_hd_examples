package com.huawei.fusioninsight.elasticsearch.example.highlevel.allrequests;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.main.MainResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.hwclient.HwRestClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.elasticsearch.index.query.QueryBuilders.matchQuery;

public class HighLevelRestClientAllRequests {
    private static final Logger LOG = LoggerFactory.getLogger(HighLevelRestClientAllRequests.class);

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
     * Create or update index by map
     */
    private static void indexByMap(RestHighLevelClient highLevelClient, String index, String type, String id) {
        try {
            Map<String, Object> dataMap = new HashMap<>();
            dataMap.put("user", "kimchy2");
            dataMap.put("age", "200");
            dataMap.put("postDate", new Date());
            dataMap.put("message", "trying out Elasticsearch");
            dataMap.put("reason", "daily update");
            dataMap.put("innerObject1", "Object1");
            dataMap.put("innerObject2", "Object2");
            dataMap.put("innerObject3", "Object3");
            dataMap.put("uid", "22");
            IndexRequest indexRequest = new IndexRequest(index, type, id).source(dataMap);
            IndexResponse indexResponse = highLevelClient.index(indexRequest, RequestOptions.DEFAULT);

            LOG.info("IndexByMap response is {}.", indexResponse.toString());
        } catch (Exception e) {
            LOG.error("IndexByMap is failed,exception occurred.", e);
        }
    }

    /**
     * Create or update index by XContentBuilder
     */
    private static void indexByXContentBuilder(RestHighLevelClient highLevelClient, String index, String type, String id) {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            {
                builder.field("user", "kimchy3");
                builder.field("age", "300");
                builder.field("postDate", "2020-01-01");
                builder.field("message", "trying out Elasticsearch");
                builder.field("reason", "daily update");
                builder.field("innerObject1", "Object1");
                builder.field("innerObject2", "Object2");
                builder.field("innerObject3", "Object3");
                builder.field("uid", "33");

            }
            builder.endObject();
            IndexRequest indexRequest = new IndexRequest(index, type, id).source(builder);
            IndexResponse indexResponse = highLevelClient.index(indexRequest, RequestOptions.DEFAULT);

            LOG.info("IndexByXContentBuilder response is {}", indexResponse.toString());
        } catch (Exception e) {
            LOG.error("IndexByXContentBuilder is failed,exception occurred.", e);
        }
    }

    /**
     * Update index
     */
    private static void update(RestHighLevelClient highLevelClient, String index, String type, String id) {
        try {

            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            {   // update information
                builder.field("postDate", new Date());
                builder.field("reason", "update again");
            }
            builder.endObject();
            UpdateRequest request = new UpdateRequest(index, type, id).doc(builder);
            UpdateResponse updateResponse = highLevelClient.update(request, RequestOptions.DEFAULT);

            LOG.info("Update response is {}.", updateResponse.toString());
        } catch (Exception e) {
            LOG.error("Update is failed,exception occurred.", e);
        }
    }

    /**
     * Bulk request can be used to to execute multiple index,update or delete
     * operations using a single request.
     */
    private static void bulk(RestHighLevelClient highLevelClient, String index, String type, String id) {

        try {
            BulkRequest request = new BulkRequest();
            Map<String, Object> jsonMap = new HashMap<>();
            jsonMap.put("user", "Linda");
            jsonMap.put("age", ThreadLocalRandom.current().nextInt(18, 100));
            jsonMap.put("postDate", "2020-01-01");
            jsonMap.put("height", (float)ThreadLocalRandom.current().nextInt(140, 220));
            jsonMap.put("weight", (float)ThreadLocalRandom.current().nextInt(70, 200));

            request.add(new IndexRequest(index, type, id).source(jsonMap));
            request.add(new UpdateRequest(index, type, id).doc(XContentType.JSON, "field", "test information"));
            request.add(new DeleteRequest(index, type, id));
            BulkResponse bulkResponse = highLevelClient.bulk(request, RequestOptions.DEFAULT);

            if (RestStatus.OK.equals((bulkResponse.status()))) {
                LOG.info("Bulk is successful");
            } else {
                LOG.info("Bulk is failed");
            }
        } catch (Exception e) {
            LOG.error("Bulk is failed,exception occurred.", e);
        }
    }

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

    /**
     * Search some information in index
     */
    private static void search(RestHighLevelClient highLevelClient, String index) {
        try {
            SearchRequest searchRequest = new SearchRequest(index);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.termQuery("user", "kimchy1"));
            //Specifying Sorting
            searchSourceBuilder.sort(new FieldSortBuilder("_doc").order(SortOrder.ASC));
            // Source filter
            String[] includeFields = new String[] {"message", "user", "innerObject*"};
            String[] excludeFields = new String[] {"postDate"};
            // Control which fields get included or excluded Request Highlighting
            searchSourceBuilder.fetchSource(includeFields, excludeFields);
            HighlightBuilder highlightBuilder = new HighlightBuilder();
            // Create a field highlighter for the user field
            HighlightBuilder.Field highlightUser = new HighlightBuilder.Field("user");
            highlightBuilder.field(highlightUser);
            searchSourceBuilder.highlighter(highlightBuilder);
            searchSourceBuilder.from(0);
            searchSourceBuilder.size(2);
            searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
            searchRequest.source(searchSourceBuilder);
            SearchResponse searchResponse = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            LOG.info("SearchIndex response is {}.", searchResponse.toString());
        } catch (Exception e) {
            LOG.error("SearchIndex is failed,exception occurred.", e);
        }
    }

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

    /**
     * Delete the index
     */
    private static void deleteIndex(RestHighLevelClient highLevelClient, String index) {
        try {
            DeleteIndexRequest request = new DeleteIndexRequest(index);
            AcknowledgedResponse delateResponse = highLevelClient.indices().delete(request, RequestOptions.DEFAULT);

            if (delateResponse.isAcknowledged()) {
                LOG.info("Delete index is successful");
            } else {
                LOG.info("Delete index is failed");
            }
        } catch (Exception e) {
            LOG.error("Delete index : {} is failed, exception occurred.", index, e);
        }
    }

    public static void main(String[] args) {

        LOG.info("Start to do high level rest client request !");
        RestHighLevelClient highLevelClient = null;
        HwRestClient hwRestClient = new HwRestClient();
        try {
            highLevelClient = new RestHighLevelClient(hwRestClient.getRestClientBuilder());
            queryClusterInfo(highLevelClient);
            indexByJson(highLevelClient, "huawei", "type1", "1");
            indexByMap(highLevelClient, "huawei", "type1", "2");
            indexByXContentBuilder(highLevelClient, "huawei", "type1", "3");
            update(highLevelClient, "huawei", "type1", "1");
            bulk(highLevelClient, "huawei", "type1", "1");
            getIndex(highLevelClient, "huawei", "type1", "1");
            search(highLevelClient, "huawei");
            String scroollId = searchScroll(highLevelClient, "huawei");
            clearScroll(highLevelClient, scroollId);
            deleteIndex(highLevelClient, "huawei");
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
