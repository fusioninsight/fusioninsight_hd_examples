/*
 * Copyright Notice:
 *      Copyright  1998-2013, Huawei Technologies Co., Ltd.  ALL Rights Reserved.
 *
 *      Warning: This computer software sourcecode is protected by copyright law
 *      and international treaties. Unauthorized reproduction or distribution
 *      of this sourcecode, or any portion of it, may result in severe civil and
 *      criminal penalties, and will be prosecuted to the maximum extent
 *      possible under the law.
 */
package com.huawei.fusioninsight.solr.example;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient.Builder;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.ZkStateReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.client.ZKClientConfig;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

public class TestSample {
    private static final Logger LOG = LoggerFactory.getLogger(TestSample.class);
    private boolean solrKbsEnable;
    private int zkClientTimeout;
    private int zkConnectTimeout;
    private String zkHost;
    private String zookeeperDefaultServerPrincipal;
    private String collectionName;
    private String defaultConfigName;
    private int shardNum;
    private int replicaNum;
    private String principal;
    private Boolean sharedFsReplication;
    private int maxShardsPerNode;
    private boolean autoAddReplicas;
    private boolean assignToSpecifiedNodeSet;
    private String createNodeSet;
    private boolean isNeedZkClientConfig;
    private ZKClientConfig zkClientConfig;

    public static void main(String[] args) throws SolrException {
        TestSample testSample = new TestSample();

        testSample.initProperties();
        if (testSample.solrKbsEnable) {
            testSample.setSecConfig();
        }

        CloudSolrClient cloudSolrClient = null;
        try {
            cloudSolrClient = testSample.getCloudSolrClient(testSample.zkHost);

            List<String> collectionNames = testSample.queryAllCollections(cloudSolrClient);

            if (collectionNames.contains(testSample.collectionName)) {
                testSample.deleteCollection(cloudSolrClient);
            }

            testSample.createCollection(cloudSolrClient);

            cloudSolrClient.setDefaultCollection(testSample.collectionName);

            testSample.addDocs(cloudSolrClient);

            testSample.addDocs2(cloudSolrClient);

            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
            }

            testSample.queryIndex(cloudSolrClient);

            testSample.removeIndex(cloudSolrClient);

            testSample.queryIndex(cloudSolrClient);

        } catch (SolrException e) {
            throw new SolrException(e.getMessage());
        } finally {
            if (cloudSolrClient != null) {
                try {
                    cloudSolrClient.close();
                } catch (IOException e) {
                    LOG.warn("Failed to close cloudSolrClient", e);
                }
            }
        }

    }

    private void initProperties() throws SolrException {
        Properties properties = new Properties();
        String proPath = System.getProperty("user.dir") + File.separator + "conf" + File.separator + "solr-example.properties";
        try {
            properties.load(new FileInputStream(new File(proPath)));
        } catch (IOException e) {
            throw new SolrException("Failed to load properties file : " + proPath);
        }
        solrKbsEnable = Boolean.parseBoolean(properties.getProperty("SOLR_KBS_ENABLED"));
        zkClientTimeout = Integer.valueOf(properties.getProperty("zkClientTimeout"));
        zkConnectTimeout = Integer.valueOf(properties.getProperty("zkConnectTimeout"));
        zkHost = properties.getProperty("zkHost");
        zookeeperDefaultServerPrincipal = properties.getProperty("ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL");
        collectionName = properties.getProperty("COLLECTION_NAME");
        defaultConfigName = properties.getProperty("DEFAULT_CONFIG_NAME");
        shardNum = Integer.valueOf(properties.getProperty("shardNum"));
        replicaNum = Integer.valueOf(properties.getProperty("replicaNum"));
        principal = properties.getProperty("principal");
        maxShardsPerNode = Integer.valueOf(properties.getProperty("maxShardsPerNode"));
        autoAddReplicas = Boolean.parseBoolean(properties.getProperty("autoAddReplicas"));
        sharedFsReplication = properties.getProperty("sharedFsReplication") == null ? null : Boolean.parseBoolean(properties.getProperty("sharedFsReplication"));
        assignToSpecifiedNodeSet = Boolean.parseBoolean(properties.getProperty("assignToSpecifiedNodeSet"));
        createNodeSet = properties.getProperty("createNodeSet");
        isNeedZkClientConfig = Boolean.parseBoolean(properties.getProperty("isNeedZkClientConfig"));
    }

    private void setSecConfig() throws SolrException {
        String path = System.getProperty("user.dir") + File.separator + "conf" + File.separator;
        path = path.replace("\\", "\\\\");

        // The following statement implys that you can use SolrClient section in jaas.conf
        // System.setProperty("solr.kerberos.jaas.appname", "SolrClient");
        try {
            LoginUtil.setJaasFile(principal, path + "user.keytab");
            LoginUtil.setKrb5Config(path + "krb5.conf");
            LoginUtil.setZookeeperServerPrincipal(zookeeperDefaultServerPrincipal);
            if (this.isNeedZkClientConfig) {
                this.setZKClientConfig();
            }
        } catch (IOException e) {
            LOG.error("Failed to set security conf", e);
            throw new SolrException("Failed to set security conf");
        }

    }

    private void setZKClientConfig() throws SolrException {
        zkClientConfig = new ZKClientConfig();
        try {
            zkClientConfig.setProperty(ZKClientConfig.LOGIN_CONTEXT_NAME_KEY, "SolrClient");
            zkClientConfig.setProperty(ZKClientConfig.ENABLE_CLIENT_SASL_KEY, "true");
            zkClientConfig.setProperty(ZKClientConfig.ZOOKEEPER_SERVER_PRINCIPAL, "zookeeper/HADOOP.HADOOP.COM");
        } catch (Exception e) {
            LOG.error("Failed to set ZKClientConfig conf", e);
            throw new SolrException("Failed to set ZKClientConfig conf");
        }
    }

    private CloudSolrClient getCloudSolrClient(String zkHost) throws SolrException {
        Builder builder = new Builder();
        builder.withZkHost(zkHost);
        CloudSolrClient cloudSolrClient = builder.build();

        cloudSolrClient.setZkClientTimeout(zkClientTimeout);
        cloudSolrClient.setZkConnectTimeout(zkConnectTimeout);
        cloudSolrClient.setisNeedZkClientConfig(isNeedZkClientConfig);
        if (isNeedZkClientConfig) {
            cloudSolrClient.setZkClientConfig(zkClientConfig);
        }
        cloudSolrClient.connect();
        LOG.info("The cloud Server has been connected !!!!");

        ZkStateReader zkStateReader = cloudSolrClient.getZkStateReader();
        ClusterState cloudState = zkStateReader.getClusterState();
        LOG.info("The zookeeper state is : {}", cloudState);

        return cloudSolrClient;
    }

    private void queryIndex(CloudSolrClient cloudSolrClient) throws SolrException {
        SolrQuery query = new SolrQuery();
        query.setQuery("name:Luna*");

        try {
            QueryResponse response = cloudSolrClient.query(query);
            SolrDocumentList docs = response.getResults();
            LOG.info("Query wasted time : {}ms", response.getQTime());

            LOG.info("Total doc num : {}", docs.getNumFound());
            for (SolrDocument doc : docs) {
                LOG.info("doc detail : " + doc.getFieldValueMap());
            }
        } catch (SolrServerException e) {
            LOG.error("Failed to query document", e);
            throw new SolrException("Failed to query document");
        } catch (IOException e) {
            LOG.error("Failed to query document", e);
            throw new SolrException("Failed to query document");
        } catch (Exception e) {
            LOG.error("Failed to query document", e);
            throw new SolrException("unknown exception");
        }
    }

    private void removeIndex(CloudSolrClient cloudSolrClient) throws SolrException {
        try {
            cloudSolrClient.deleteByQuery("*:*");
            LOG.info("Success to delete index");
        } catch (SolrServerException e) {
            LOG.error("Failed to remove document", e);
            throw new SolrException("Failed to remove document");
        } catch (IOException e) {
            LOG.error("Failed to remove document", e);
            throw new SolrException("Failed to remove document");
        }
    }

    private void addDocs(CloudSolrClient cloudSolrClient) throws SolrException {
        Collection<SolrInputDocument> documents = new ArrayList<SolrInputDocument>();
        for (Integer i = 0; i < 5; i++) {
            SolrInputDocument doc = new SolrInputDocument();
            doc.addField("id", i.toString());
            doc.addField("name", "Luna_" + i);
            doc.addField("features", "test" + i);
            doc.addField("price", (float) i * 1.01);
            documents.add(doc);
        }
        try {
            cloudSolrClient.add(documents);
            LOG.info("success to add index");
        } catch (SolrServerException e) {
            LOG.error("Failed to add document to collection", e);
            throw new SolrException("Failed to add document to collection");
        } catch (IOException e) {
            LOG.error("Failed to add document to collection", e);
            throw new SolrException("Failed to add document to collection");
        } catch (Exception e) {
            LOG.error("Failed to add document to collection", e);
            throw new SolrException("unknown exception");
        }
    }

    private void addDocs2(CloudSolrClient cloudSolrClient) throws SolrException {
        UpdateRequest request = new UpdateRequest();
        Collection<SolrInputDocument> documents = new ArrayList<SolrInputDocument>();
        for (Integer i = 5; i < 10; i++) {
            SolrInputDocument doc = new SolrInputDocument();
            doc.addField("id", i.toString());
            doc.addField("name", "Luna_" + i);
            doc.addField("features", "test" + i);
            doc.addField("price", (float) i * 1.01);
            documents.add(doc);
        }
        request.add(documents);
        try {
            cloudSolrClient.request(request);
        } catch (SolrServerException e) {
            LOG.error("Failed to add document to collection", e);
            throw new SolrException("Failed to add document to collection");
        } catch (IOException e) {
            LOG.error("Failed to add document to collection", e);
            throw new SolrException("Failed to add document to collection");
        } catch (Exception e) {
            LOG.error("Failed to add document to collection", e);
            throw new SolrException("unknown exception");
        }
    }

    private void createCollection(CloudSolrClient cloudSolrClient) throws SolrException {
        CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collectionName, defaultConfigName, shardNum, replicaNum);
        create.setSharedFsReplication(sharedFsReplication);
        create.setMaxShardsPerNode(maxShardsPerNode);
        create.setAutoAddReplicas(autoAddReplicas);
        if (assignToSpecifiedNodeSet) {
            create.setCreateNodeSet(createNodeSet);
        }
        CollectionAdminResponse response = null;
        try {
            response = create.process(cloudSolrClient);
        } catch (SolrServerException e) {
            LOG.error("Failed to create collection", e);
            throw new SolrException("Failed to create collection");
        } catch (IOException e) {
            LOG.error("Failed to create collection", e);
            throw new SolrException("Failed to create collection");
        } catch (Exception e) {
            LOG.error("Failed to create collection", e);
            throw new SolrException("unknown exception");
        }
        if (response.isSuccess()) {
            LOG.info("Success to create collection[{}]", collectionName);
        } else {
            LOG.error("Failed to create collection[{}], cause : {}", collectionName, response.getErrorMessages());
            throw new SolrException("Failed to create collection");
        }
    }

    private void deleteCollection(CloudSolrClient cloudSolrClient) throws SolrException {
        CollectionAdminRequest.Delete delete = CollectionAdminRequest.deleteCollection(collectionName);
        CollectionAdminResponse response = null;
        try {
            response = delete.process(cloudSolrClient);
        } catch (SolrServerException e) {
            LOG.error("Failed to delete collection", e);
            throw new SolrException("Failed to create collection");
        } catch (IOException e) {
            LOG.error("Failed to delete collection", e);
            throw new SolrException("unknown exception");
        } catch (Exception e) {
            LOG.error("Failed to delete collection", e);
            throw new SolrException("unknown exception");
        }
        if (response.isSuccess()) {
            LOG.info("Success to delete collection[{}]", collectionName);
        } else {
            LOG.error("Failed to delete collection[{}], cause : {}", collectionName, response.getErrorMessages());
            throw new SolrException("Failed to delete collection");
        }
    }

    @SuppressWarnings("unchecked")
    private List<String> queryAllCollections(CloudSolrClient cloudSolrClient) throws SolrException {
        CollectionAdminRequest.List list = new CollectionAdminRequest.List();
        CollectionAdminResponse listRes = null;
        try {
            listRes = list.process(cloudSolrClient);
        } catch (SolrServerException e) {
            LOG.error("Failed to list collection", e);
            throw new SolrException("Failed to list collection");
        } catch (IOException e) {
            LOG.error("Failed to list collection", e);
            throw new SolrException("Failed to list collection");
        } catch (Exception e) {
            LOG.error("Failed to list collection", e);
            throw new SolrException("unknown exception");
        }
        List<String> collectionNames = (List<String>) listRes.getResponse().get("collections");
        LOG.info("All existed collections : {}", collectionNames);
        return collectionNames;
    }
}
