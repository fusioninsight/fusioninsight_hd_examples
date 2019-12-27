package com.huawei.fusioninsight.elasticsearch.example.indices;

import java.util.concurrent.ExecutionException;

import com.huawei.fusioninsight.elasticsearch.example.util.CommonUtil;
import com.huawei.fusioninsight.elasticsearch.transport.client.PreBuiltHWTransportClient;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.settings.Settings;

public class IndicesSample {
    private static final Logger LOG = LogManager.getLogger(IndicesSample.class);

    public static void createIndexWithSettings(PreBuiltHWTransportClient client, String indexName) {
        GetIndexResponse response;
        try {
            client.prepare()
                .admin()
                .indices()
                .prepareCreate(indexName)
                .setSettings(Settings.builder().put("index.number_of_shards", 3).put("index.number_of_replicas", 1))
                .get();
            response = client.prepare().admin().indices().prepareGetIndex().get();
        } catch (ElasticsearchSecurityException e) {
            CommonUtil.handleException(e);
            return;
        }
        LOG.info(response.settings());
    }

    public static void createIndexWithMapping(PreBuiltHWTransportClient client, String indexName) {
        GetIndexResponse response;
        try {
            client.prepare().admin().indices().prepareCreate(indexName).addMapping("tweet", "message", "type=text").get();
            response = client.prepare().admin().indices().prepareGetIndex().get();
        } catch (ElasticsearchSecurityException e) {
            CommonUtil.handleException(e);
            return;
        }
        LOG.info(response.mappings().get(indexName).get("tweet").source());
    }

    public static void deleteIndices(PreBuiltHWTransportClient client, String indices) {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(indices);
        AcknowledgedResponse response;
        try {
            response = client.prepare().admin().indices().delete(deleteIndexRequest).get();
        } catch (ElasticsearchSecurityException | ExecutionException | InterruptedException e) {
            CommonUtil.handleException(e);
            return;
        }
        if (response.isAcknowledged()) {
            LOG.info("Delete success!");
        }
    }

}
