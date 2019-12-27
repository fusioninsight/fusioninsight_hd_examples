package com.huawei.fusioninsight.elasticsearch.example.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.index.IndexResponse;

public class CommonUtil {
    private static final Logger LOG = LogManager.getLogger(CommonUtil.class);

    public static void handleException(ElasticsearchSecurityException e) {
        LOG.error("Your permissions are incorrect," + e.getMessage());
    }
    public static void handleException(Exception e) {
        LOG.error("Your permissions are incorrect," + e.getMessage());
    }

    public static void printIndexInfo(IndexResponse response) {
        String _index = response.getIndex();
        String _type = response.getType();
        String _id = response.getId();
        long _version = response.getVersion();
        LOG.info(_index + "," + _type + "," + _id + "," + _version);
    }

}
