package com.huawei.fusioninsight.elasticsearch.example;

import com.huawei.fusioninsight.elasticsearch.example.cluster.ClusterSample;
import com.huawei.fusioninsight.elasticsearch.example.document.MultiDocumentSample;
import com.huawei.fusioninsight.elasticsearch.example.document.SingleDocumentSample;
import com.huawei.fusioninsight.elasticsearch.example.indices.IndicesSample;
import com.huawei.fusioninsight.elasticsearch.example.search.SearchSample;
import com.huawei.fusioninsight.elasticsearch.transport.client.ClientFactory;
import com.huawei.fusioninsight.elasticsearch.transport.client.PreBuiltHWTransportClient;

public class Sample {

    private static PreBuiltHWTransportClient client;

    public static void main(String[] args) throws Exception {
        ClientFactory.initConfiguration(LoadProperties.loadProperties());
        client = ClientFactory.getClient();
        ClusterSample.clusterHealth(client);
        IndicesSample.createIndexWithSettings(client, "article");
        IndicesSample.createIndexWithMapping(client,"tweet");
        SingleDocumentSample.createBeanDocument(client);
        SingleDocumentSample.getDocument(client, "article", "article", "1");
        ClusterSample.clusterHealth(client);
        SingleDocumentSample.createMapDocument(client);
        SingleDocumentSample.createDocumentID(client, "book", "book", "1");
        SingleDocumentSample.createDocumentID(client, "book", "book", "2");
        SingleDocumentSample.createJsonStringDocument(client);
        SingleDocumentSample.createDocumentID(client, "book", "book", "10");
        MultiDocumentSample.bulkDocuments(client);
        SearchSample.scrollSearchDelete(client, "article", "content", "elasticsearch");
        SingleDocumentSample.getDocument(client, "book", "book", "2");
        SingleDocumentSample.getDocument(client, "article", "article", "1");

        SearchSample.multiSearch(client, "lucene");
        SearchSample.matchQuery(client, "book", "desc", "full-text");
        SearchSample.booleanQuery(client);
        SearchSample.fuzzyLikeQuery(client);
        SearchSample.matchAllQuery(client);
        SearchSample.prefixQuery(client);
        SearchSample.queryString(client);
        SearchSample.rangeQuery(client);
        SearchSample.termsQuery(client);
        SearchSample.wildcardQuery(client);
        SearchSample.regexpQuery(client);

        SingleDocumentSample.deleteDocument(client, "book", "book", "1");
        IndicesSample.deleteIndices(client, "book");
        IndicesSample.deleteIndices(client, "article");
        IndicesSample.deleteIndices(client, "tweet");
        client.close();
    }

}
