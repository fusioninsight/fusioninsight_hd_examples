package com.huawei.fusioninsight.elasticsearch.example.search;

import com.huawei.fusioninsight.elasticsearch.example.util.CommonUtil;
import com.huawei.fusioninsight.elasticsearch.transport.client.PreBuiltHWTransportClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.util.Map;
import java.util.Set;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;

public class SearchSample {
    private static final Logger LOG = LogManager.getLogger(SearchSample.class);

    public static void scrollSearchDelete(PreBuiltHWTransportClient client, String index, String name, String value) {
        LOG.info("scrollSearchDelete:");
        try {
            QueryBuilder qb = termQuery(name, value);
            SearchResponse scrollResp = client.prepare().prepareSearch(index).addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
                .setScroll(new TimeValue(60000)).setQuery(qb).setSize(100).execute().actionGet();
            //100 hits per shard will be returned for each scroll
            BulkRequestBuilder bulkRequest = client.prepare().prepareBulk();

            while (true) {
                for (SearchHit hit : scrollResp.getHits().getHits()) {
                    LOG.info(hit.getIndex() + hit.getType());
                    LOG.info(hit.getSourceAsString());
                    bulkRequest.add(client.prepare().prepareDelete(hit.getIndex(), hit.getType(), hit.getId()));
                }
                scrollResp =
                    client.prepare().prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
                if (scrollResp.getHits().getHits().length == 0) {
                    break;
                }
            }
            if (bulkRequest.numberOfActions() == 0) {
                return;
            }
            BulkResponse bulkResponse = bulkRequest.get();
            BulkItemResponse[] bulkItemResponses = bulkResponse.getItems();
            for (BulkItemResponse bulkItemResponse : bulkItemResponses) {
                LOG.info("index:" + bulkItemResponse.getIndex());
                LOG.info("type:" + bulkItemResponse.getType());
                LOG.info("Optype:" + bulkItemResponse.getOpType());
                LOG.info("isFailed:" + bulkItemResponse.isFailed());
            }
        } catch (ElasticsearchSecurityException e) {
            CommonUtil.handleException(e);
        }
    }

    public static void termsQuery(PreBuiltHWTransportClient client) {
        TermsQueryBuilder termsQueryBuilder = QueryBuilders.termsQuery("content", "elasticsearch", "alex");
        SearchResponse searchResponse;
        try {
            searchResponse =
                client.prepare().prepareSearch("article").setQuery(termsQueryBuilder).setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                    .setFrom(0).setSize(60).setExplain(true).execute().actionGet();
        } catch (ElasticsearchSecurityException e) {
            CommonUtil.handleException(e);
            return;
        }
        SearchHits searchHits = searchResponse.getHits();
        LOG.info("termsQuery:");
        LOG.info("Total match found:" + searchHits.getTotalHits());
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit searchHit : hits) {
            LOG.info(searchHit.getSourceAsString());
            //Get the highlighting field
            Map<String, HighlightField> highlightFields = searchHit.getHighlightFields();
            if (0 == highlightFields.size()) {
                return;
            }
            HighlightField highlightField = highlightFields.get("content");
            LOG.info("Highlighting field:" + highlightField.getName() + "\nHighlighting field content:" + highlightField.getFragments()[0]
                .string());
            Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
            Set<String> keySet = sourceAsMap.keySet();
            for (String string : keySet) {
                LOG.info(string + ":" + sourceAsMap.get(string));
            }
        }
    }

    public static void matchQuery(PreBuiltHWTransportClient client, String indices, String field, String queryString) {
        SearchResponse searchResponse;
        try {
            searchResponse =
                client.prepare().prepareSearch(indices).setQuery(QueryBuilders.matchQuery(field, queryString)).execute().actionGet();

        } catch (ElasticsearchSecurityException e) {
            CommonUtil.handleException(e);
            return;
        }
        SearchHits searchHits = searchResponse.getHits();
        LOG.info("matchquery [" + field + "] [" + queryString + "]:");
        LOG.info("Total match found:" + searchHits.getTotalHits());
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit searchHit : hits) {
            Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
            Set<String> keySet = sourceAsMap.keySet();
            for (String string : keySet) {
                LOG.info(string + ":" + sourceAsMap.get(string));
            }
        }
    }

    public static void regexpQuery(PreBuiltHWTransportClient client) {
        RegexpQueryBuilder regexpQuery = QueryBuilders.regexpQuery("content", "Elasticsearch|Lucene");
        SearchResponse searchResponse;
        try {
            searchResponse =
                client.prepare().prepareSearch("article").setQuery(regexpQuery).setFrom(0).setSize(60).setExplain(true).execute()
                    .actionGet();
        } catch (ElasticsearchSecurityException e) {
            CommonUtil.handleException(e);
            return;
        }
        SearchHits searchHits = searchResponse.getHits();
        LOG.info("regexpQuery:");
        LOG.info("Total match found:" + searchHits.getTotalHits());
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit searchHit : hits) {
            //Get the highlighting field
            Map<String, HighlightField> highlightFields = searchHit.getHighlightFields();
            HighlightField highlightField = highlightFields.get("content");
            LOG.info("Highlighting field:" + highlightField.getName() + "\nHighlighting field content:" + highlightField.getFragments()[0]
                .string());
            Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
            Set<String> keySet = sourceAsMap.keySet();
            for (String string : keySet) {
                LOG.info(string + ":" + sourceAsMap.get(string));
            }
        }
    }

    public static void wildcardQuery(PreBuiltHWTransportClient client) {
        WildcardQueryBuilder wildcardQuery = QueryBuilders.wildcardQuery("content", "S?");
        SearchResponse searchResponse;
        try {
            searchResponse =
                client.prepare().prepareSearch("article").setQuery(wildcardQuery).setFrom(0).setSize(60).setExplain(true).execute()
                    .actionGet();
        } catch (ElasticsearchSecurityException e) {
            CommonUtil.handleException(e);
            return;
        }
        SearchHits searchHits = searchResponse.getHits();
        LOG.info("wildcardQuery:");
        LOG.info("Total match found:" + searchHits.getTotalHits());
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit searchHit : hits) {
            //Get the highlighting field
            Map<String, HighlightField> highlightFields = searchHit.getHighlightFields();
            HighlightField highlightField = highlightFields.get("content");
            LOG.info("Highlighting field:" + highlightField.getName() + "\nHighlighting field content:" + highlightField.getFragments()[0]
                .string());
            Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
            Set<String> keySet = sourceAsMap.keySet();
            for (String string : keySet) {
                LOG.info(string + ":" + sourceAsMap.get(string));
            }
        }
    }

    public static void rangeQuery(PreBuiltHWTransportClient client) {
        RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery("id").from(1).to(3).includeLower(true).includeUpper(false);
        SearchResponse searchResponse;
        try {
            searchResponse =
                client.prepare().prepareSearch("article").setQuery(rangeQuery).setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setFrom(0)
                    .setSize(60).setExplain(true).execute().actionGet();
        } catch (ElasticsearchSecurityException e) {
            CommonUtil.handleException(e);
            return;
        }
        SearchHits searchHits = searchResponse.getHits();
        LOG.info("rangeQuery:");
        LOG.info("Total match found:" + searchHits.getTotalHits());
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit searchHit : hits) {
            //Get the highlighting field
            Map<String, HighlightField> highlightFields = searchHit.getHighlightFields();
            HighlightField highlightField = highlightFields.get("content");
            if (null != highlightField) {
                LOG.info(
                    "Highlighting field:" + highlightField.getName() + "\nHighlighting field content:" + highlightField.getFragments()[0]
                        .string());
            }
            Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
            Set<String> keySet = sourceAsMap.keySet();
            for (String string : keySet) {
                LOG.info(string + ":" + sourceAsMap.get(string));
            }
        }
    }

    public static void queryString(PreBuiltHWTransportClient client) {
        SearchResponse searchResponse;
        try {
            searchResponse = client.prepare().prepareSearch("article").setQuery(QueryBuilders.queryStringQuery("Elastichsearch Beijing"))
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setFrom(0).setSize(60).setExplain(true).execute().actionGet();
        } catch (ElasticsearchSecurityException e) {
            CommonUtil.handleException(e);
            return;
        }
        SearchHits searchHits = searchResponse.getHits();
        LOG.info("queryString:");
        LOG.info("Total match found:" + searchHits.getTotalHits());
        LOG.info(searchHits.toString());
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit searchHit : hits) {
            //Get the highlighting field
            LOG.info(searchHit);
            Map<String, HighlightField> highlightFields = searchHit.getHighlightFields();
            LOG.info(highlightFields);
            HighlightField highlightField = highlightFields.get("content");
            LOG.info(highlightField);
            LOG.info("Highlighting field:" + highlightField.getName() + "\nHighlighting field content:" + highlightField.getFragments()[0]
                .string());
            Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
            Set<String> keySet = sourceAsMap.keySet();
            for (String string : keySet) {
                LOG.info(string + ":" + sourceAsMap.get(string));
            }
        }
    }

    public static void prefixQuery(PreBuiltHWTransportClient client) {
        SearchResponse searchResponse;
        try {
            searchResponse = client.prepare().prepareSearch("article").setQuery(QueryBuilders.prefixQuery("title", "Lucene In Action"))
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setFrom(0).setSize(60).setExplain(true).execute().actionGet();
        } catch (ElasticsearchSecurityException e) {
            CommonUtil.handleException(e);
            return;
        }
        SearchHits searchHits = searchResponse.getHits();
        LOG.info("prefixQuery:");
        LOG.info("Total match found:" + searchHits.getTotalHits());
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit searchHit : hits) {
            //Get the highlighting field
            Map<String, HighlightField> highlightFields = searchHit.getHighlightFields();
            HighlightField highlightField = highlightFields.get("content");
            LOG.info("Highlighting field:" + highlightField.getName() + "\nHighlighting field content:" + highlightField.getFragments()[0]
                .string());
            Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
            Set<String> keySet = sourceAsMap.keySet();
            for (String string : keySet) {
                LOG.info(string + ":" + sourceAsMap.get(string));
            }
        }
    }

    public static void matchAllQuery(PreBuiltHWTransportClient client) {
        SearchResponse searchResponse;
        try {
            searchResponse = client.prepare().prepareSearch("book").setQuery(QueryBuilders.matchAllQuery())
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setFrom(0).setSize(60).setExplain(true).execute().actionGet();
        } catch (ElasticsearchSecurityException e) {
            CommonUtil.handleException(e);
            return;
        }
        SearchHits searchHits = searchResponse.getHits();
        LOG.info("matchAllQuery:");
        LOG.info("Total match found:" + searchHits.getTotalHits());
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit searchHit : hits) {
            Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
            Set<String> keySet = sourceAsMap.keySet();
            for (String string : keySet) {
                LOG.info(string + ":" + sourceAsMap.get(string));
            }
        }
    }

    public static void fuzzyLikeQuery(PreBuiltHWTransportClient client) {
        SearchResponse searchResponse;
        try {
            searchResponse = client.prepare().prepareSearch("book").setQuery(QueryBuilders.fuzzyQuery("desc", "full-text"))
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setFrom(0).setSize(60).setExplain(true).execute().actionGet();
        } catch (ElasticsearchSecurityException e) {
            CommonUtil.handleException(e);
            return;
        }
        SearchHits searchHits = searchResponse.getHits();
        LOG.info("fuzzyLikeQuery:");
        LOG.info("Total match found:" + searchHits.getTotalHits());
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit searchHit : hits) {
            //Get the highlighting field
            Map<String, HighlightField> highlightFields = searchHit.getHighlightFields();
            HighlightField highlightField = highlightFields.get("content");
            LOG.info("Highlighting field:" + highlightField.getName() + "\nHighlighting field content:" + highlightField.getFragments()[0]
                .string());
            Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
            Set<String> keySet = sourceAsMap.keySet();
            for (String string : keySet) {
                LOG.info(string + ":" + sourceAsMap.get(string));
            }
        }
    }

    public static void booleanQuery(PreBuiltHWTransportClient client) {
        QueryBuilder queryBuilder = QueryBuilders.boolQuery().must(termQuery("desc", "full-text")).must(termQuery("name", "Elasticsearch"))
            .mustNot(termQuery("desc", "Lucene")).should(termQuery("desc", "open-source"));
        SearchResponse searchResponse;
        try {
            searchResponse = client.prepare().prepareSearch("book").setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setFrom(0).setSize(60)
                .setExplain(true).setQuery(queryBuilder).execute().actionGet();
        } catch (ElasticsearchSecurityException e) {
            CommonUtil.handleException(e);
            return;
        }
        SearchHits searchHits = searchResponse.getHits();
        LOG.info("boolQuery:");
        LOG.info("Total match found:" + searchHits.getTotalHits());
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit searchHit : hits) {
            //Get the highlighting field
            Map<String, HighlightField> highlightFields = searchHit.getHighlightFields();
            HighlightField deschighlightField = highlightFields.get("desc");
            LOG.info("Highlighting field:" + deschighlightField.getName() + "\nHighlighting field content:" + deschighlightField
                .getFragments()[0].string());
            HighlightField namehighlightField = highlightFields.get("name");
            LOG.info("Highlighting field:" + namehighlightField.getName() + "\nHighlighting field content:" + namehighlightField
                .getFragments()[0].string());
            Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
            Set<String> keySet = sourceAsMap.keySet();
            for (String string : keySet) {
                LOG.info(string + ":" + sourceAsMap.get(string));
            }
        }
    }

    public static void multiSearch(PreBuiltHWTransportClient client, String queryString) {
        SearchRequestBuilder srb1;
        SearchRequestBuilder srb2;
        MultiSearchResponse sr;
        try {
            srb1 = client.prepare().prepareSearch().setQuery(QueryBuilders.queryStringQuery(queryString));
            srb2 = client.prepare().prepareSearch().setQuery(QueryBuilders.matchQuery("desc", queryString));
            sr = client.prepare().prepareMultiSearch().add(srb1).add(srb2).execute().actionGet();
        } catch (ElasticsearchSecurityException e) {
            CommonUtil.handleException(e);
            return;
        }
        long nbHits = 0;
        for (MultiSearchResponse.Item item : sr.getResponses()) {
            SearchResponse response = item.getResponse();
            nbHits += response.getHits().getTotalHits();
            LOG.info("Indices found:" + nbHits);
            SearchHits searchHits = response.getHits();
            LOG.info("key:[" + queryString + "]:");
            LOG.info("Total match found:" + searchHits.getTotalHits());
            SearchHit[] hits = searchHits.getHits();
            for (SearchHit searchHit : hits) {
                Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
                Set<String> keySet = sourceAsMap.keySet();
                for (String string : keySet) {
                    LOG.info(string + ":" + sourceAsMap.get(string));
                }
            }
        }
    }

}
