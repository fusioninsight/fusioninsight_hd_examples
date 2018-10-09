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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.ZkStateReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolrUtil {
    private static final Logger LOG = LoggerFactory.getLogger(SolrUtil.class);

    public static CloudSolrClient getCloudSolrClient(String zkHost) throws SolrException {

        CloudSolrClient cloudSolrClient = new CloudSolrClient(zkHost);
        cloudSolrClient.setZkClientTimeout(30 * 1000);
        cloudSolrClient.setZkConnectTimeout(30 * 1000);
        cloudSolrClient.connect();
        LOG.info("The cloud Server has been connected !!!!");

        ZkStateReader zkStateReader = cloudSolrClient.getZkStateReader();
        ClusterState cloudState = zkStateReader.getClusterState();
        LOG.info("The zookeeper state is : {}", cloudState);

        return cloudSolrClient;
    }

    public static void queryIndex(CloudSolrClient cloudSolrClient, String queryString) throws SolrException {
        SolrQuery query = new SolrQuery();
        query.setQuery(queryString);

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


    public static void addDocs(CloudSolrClient cloudSolrClient, PeopleInfo info) throws SolrException {
        Collection<SolrInputDocument> documents = new ArrayList<SolrInputDocument>();
        try {        	
            SolrInputDocument doc = new SolrInputDocument();
            doc.addField("id", info.getName());
            doc.addField("name", info.getName());
            doc.addField("features", info.getSex());
            doc.addField("popularity", info.getAge());
            documents.add(doc);
        	
            cloudSolrClient.add(documents);
            cloudSolrClient.commit();
            LOG.info("success to add index");
        } catch (SolrServerException e) {
            LOG.error("Failed to add document to collection", e);
            throw new SolrException("Failed to add document to collection");
        } catch (IOException e){
            LOG.error("Failed to add document to collection", e);
            throw new SolrException("Failed to add document to collection");
        }
        catch (Exception e) {
            LOG.error("Failed to add document to collection", e);
            throw new SolrException("unknown exception");
        }
    }
}
