package com.huawei.fusioninsight.elasticsearch.example.lowlevel;

import com.google.gson.Gson;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.PropertyConfigurator;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.Response;
import org.elasticsearch.hwclient.HwRestClient;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/*
 *场景描述：
 * 1.获取客户端，连接Elasticsearch集群
 * 2.查询集群健康状态
 * 3.检查指定索引是否存在
 * 4.删除索引
 * 5.创建指定分片数目的索引
 * 6.写入数据
 * 7.批量写入数据
 * 8.查询数据
 * 9.删除某条数据
 * 10.删除索引
 */

public class AESDemoForDoc {
    private static final Log LOG = LogFactory.getLog(AESDemoForDoc.class.getName());
    private static Response response;
    private static RestClient restClient = null;

    static {
        //日志配置文件
        PropertyConfigurator.configure(AESDemoForDoc.class.getClassLoader().getResource("log4j.properties").getPath());
    }

    public static void main(String[] args) throws Exception {
        LOG.info("Start to do es test !");

        // 1.获取客户端，连接Elasticsearch集群
        // 6.5.1版本HwRestClient对象和getRestClient方法已经对客户端配置和安全登录做好了封装，
        // 只需要配置好conf下的esParams.properties，并添加krb5.conf和user.keytab

        HwRestClient hwRestClient = new HwRestClient();
        restClient = hwRestClient.getRestClient();
        String index = "es_test_index";

        try {
            // 2.查询集群健康状态
            Request request = new Request("GET", "/_cluster/health");
            request.addParameter("pretty", "true");
            response = restClient.performRequest(request);

            if (HttpStatus.SC_OK == response.getStatusLine().getStatusCode()) {
                LOG.info("QueryClusterInfo successful.");
            } else {
                LOG.error("QueryClusterInfo failed.");
            }
            LOG.info("QueryClusterInfo response entity is : " + EntityUtils.toString(response.getEntity()));


            //3.检查指定索引是否存在
            request = new Request("HEAD", "/" + index);
            request.addParameter("pretty", "true");
            response = restClient.performRequest(request);
            if (HttpStatus.SC_OK == response.getStatusLine().getStatusCode()) {
                LOG.info("Check index successful,index is exist : " + index);

                // 4.删除索引
                request = new Request("DELETE", "/" + index + "?&pretty=true");
                response = restClient.performRequest(request);
                if (HttpStatus.SC_OK == response.getStatusLine().getStatusCode()) {
                    LOG.info("Delete index successful.");
                } else {
                    LOG.error("Delete index failed.");
                }
                LOG.info("Delete index response entity is : " + EntityUtils.toString(response.getEntity()));
            }
            if (HttpStatus.SC_NOT_FOUND == response.getStatusLine().getStatusCode()) {
                LOG.info("Index is not exist : " + index);
            }

            // 5.创建指定分片数目的索引
            int shardNum = 3;
            int replicaNum = 1;
            String jsonString =
                    "{" + "\"settings\":{" + "\"number_of_shards\":\"" + shardNum + "\"," + "\"number_of_replicas\":\"" + replicaNum + "\"" + "}}";
            HttpEntity entity = new NStringEntity(jsonString, ContentType.APPLICATION_JSON);
            request = new Request("PUT", "/" + index);
            request.addParameter("pretty", "true");
            request.setEntity(entity);
            response = restClient.performRequest(request);
            if (HttpStatus.SC_OK == response.getStatusLine().getStatusCode()) {
                LOG.info("CreateIndexWithShardNum successful.");
            } else {
                LOG.error("CreateIndexWithShardNum failed.");
            }
            LOG.info("CreateIndexWithShardNum response entity is : " + EntityUtils.toString(response.getEntity()));


            // 6.写入数据
            String type = "type1";
            String id = "id1";
            Gson gson = new Gson();
            Map<String, Object> esMap = new HashMap<>();
            esMap.put("name", "Happy");
            esMap.put("author", "Alex Yang");
            esMap.put("pubinfo", "Beijing,China");
            esMap.put("pubtime", "2020-01-01");
            esMap.put("description", "Elasticsearch is a highly scalable open-source full-text search and analytics engine.");
            String jsonString1 = gson.toJson(esMap);
            Map<String, String> params = Collections.singletonMap("pretty", "true");
            HttpEntity entity1 = new NStringEntity(jsonString1, ContentType.APPLICATION_JSON);

            request = new Request("POST", "/" + index + "/" + type + "/" + id);
            request.addParameter("pretty", "true");
            request.setEntity(entity1);
            response = restClient.performRequest(request);
            if (HttpStatus.SC_OK == response.getStatusLine().getStatusCode() ||
                    HttpStatus.SC_CREATED == response.getStatusLine().getStatusCode()) {
                LOG.info("PutData successful.");
            } else {
                LOG.error("PutData failed.");
            }
            LOG.info("PutData response entity is : " + EntityUtils.toString(response.getEntity()));


            // 7.批量写入数据
            //需要写入的总文档数
            long totalRecordNum = 10;
            //一次bulk写入的文档数
            long oneCommit = 5;
            long circleNumber = totalRecordNum / oneCommit;
            StringEntity entity2;
            Gson gson2 = new Gson();
            Map<String, Object> esMap2 = new HashMap<>();
            String str = "{ \"index\" : { \"_index\" : \"" + index + "\", \"_type\" :  \"" + type + "\"} }";

            for (int i = 1; i <= circleNumber; i++) {
                StringBuffer buffer = new StringBuffer();
                for (int j = 1; j <= oneCommit; j++) {
                    esMap2.clear();
                    esMap2.put("name", "Linda");
                    esMap2.put("age", ThreadLocalRandom.current().nextInt(18, 100));
                    esMap2.put("height", (float) ThreadLocalRandom.current().nextInt(140, 220));
                    esMap2.put("weight", (float) ThreadLocalRandom.current().nextInt(70, 200));
                    esMap2.put("cur_time", System.currentTimeMillis());

                    String strJson = gson2.toJson(esMap2);
                    buffer.append(str).append("\n");
                    buffer.append(strJson).append("\n");
                }
                entity2 = new StringEntity(buffer.toString(), ContentType.APPLICATION_JSON);
                entity2.setContentEncoding("UTF-8");

                request = new Request("PUT", "/_bulk");
                request.setEntity(entity2);
                request.addParameter("pretty", "true");
                response = restClient.performRequest(request);
                if (HttpStatus.SC_OK == response.getStatusLine().getStatusCode()) {
                    LOG.info("Already input documents : " + oneCommit * i);
                } else {
                    LOG.error("Bulk failed.");
                }
                LOG.info("Bulk response entity is : " + EntityUtils.toString(response.getEntity()));
            }

            // 8.查询数据
            request = new Request("GET", "/" + index + "/" + type + "/" + id);
            request.addParameter("pretty", "true");
            response = restClient.performRequest(request);
            if (HttpStatus.SC_OK == response.getStatusLine().getStatusCode()) {
                LOG.info("QueryData successful.");
            } else {
                LOG.error("QueryData failed.");
            }
            LOG.info("QueryData response entity is : " + EntityUtils.toString(response.getEntity()));


            // 9.删除某条数据
            //删除指定索引 index 中字段field 值为value的数据
            String field = "pubinfo";
            String value = "Beijing";
            String jsonString3 = "{\n" + "  \"query\": {\n" + "    \"match\": { \"" + field + "\":\"" + value + "\"}\n" + "  }\n" + "}";
            HttpEntity entity3 = new NStringEntity(jsonString3, ContentType.APPLICATION_JSON);
            Response response;
            try {
                request = new Request("POST", "/" + index + "/_delete_by_query");
                request.addParameter("pretty", "true");
                request.setEntity(entity3);
                response = restClient.performRequest(request);
                if (HttpStatus.SC_OK == response.getStatusLine().getStatusCode()) {
                    LOG.info("DeleteSomeDocumentsInIndex successful.");
                } else {
                    LOG.error("DeleteSomeDocumentsInIndex failed.");
                }
                LOG.info("DeleteSomeDocumentsInIndex response entity is : " + EntityUtils.toString(response.getEntity()));
            } catch (Exception e) {
                LOG.error("DeleteSomeDocumentsInIndex failed, exception occurred.", e);
            }

            // 10.删除索引
            request = new Request("DELETE", "/" + index + "?&pretty=true");
            response = restClient.performRequest(request);
            if (HttpStatus.SC_OK == response.getStatusLine().getStatusCode()) {
                LOG.info("Delete index successful.");
            } else {
                LOG.error("Delete index failed.");
            }
            LOG.info("Delete index response entity is : " + EntityUtils.toString(response.getEntity()));

        } catch (Exception e) {
            LOG.error("There are exceptions occurred.", e);
        } finally {
            if (restClient != null) {
                try {
                    restClient.close();
                    LOG.info("Close the client successful.");
                } catch (Exception e1) {
                    LOG.error("Close the client failed.", e1);
                }
            }
        }
    }
}