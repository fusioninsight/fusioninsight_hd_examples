package com.huawei.bigdata.examples.manager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpEntity;
import org.apache.http.client.CookieStore;
import org.apache.http.client.methods.*;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.PropertyConfigurator;

import javax.xml.bind.DatatypeConverter;

//对应接口文档《FusionInsight V100R002C80SPC100 集群管理对外接口文档（监控）.docx》
//因为Manager使用的是HTTPS协议，需要参考《配置HTTPS证书校验》将服务端证书添加到客户端的信任列表中

public class AManagerDemoForDoc {

    static {
        //日志配置文件
        PropertyConfigurator.configure(AManagerDemoForDoc.class.getClassLoader().getResource("conf/log4j.properties").getPath());
    }
    private final static Log LOG = LogFactory.getLog(AManagerDemoForDoc.class.getName());

    private static void printResponse(CloseableHttpResponse httpResponse) throws Exception
    {
        HttpEntity entity =  httpResponse.getEntity();

        LOG.info("----------------Status Start------------------------");
        LOG.info(httpResponse.getStatusLine());

        LOG.info("---------------EntityUtils Start-------------------------");
        if (entity != null) {
            LOG.info("---------------entity is not empty-------------------------");
            //返回的告警信息为JSON串，字段的解读可以参考接口文档
            String entiryString = EntityUtils.toString(entity);
            System.out.println(entiryString);
        }

        LOG.info("----------EntityUtilsconsume Start------------------------------");
        EntityUtils.consume(entity);
    }

    public static void main(String[] args) throws Exception
    {
        //IP地址替换为对接环境的实际地址
        String ipAddr = "189.211.69.122";

        //替换为需要查询监控信息的服务名称
        String serviceName = "HDFS";

        String url;

        //*******************查询HDFS服务当前支持的所有监控指标************************
        //GET  https://”host”:”port”/web/monitor/service/customize/{clusterID}/{serviceName}.do

        url = "https://" + ipAddr + ":28443/web/monitor/service/customize/1/" + serviceName + ".do";
        HttpGet httpGet = new HttpGet(url);

        //首次请求使用HTTP Basic认证，用户名和密码需要根据实际环境修改
        String authStr = "admin" + ":" + "Huawei@123";
        //根据协议要求，用户名密码需要使用base64编码
        String encoding = DatatypeConverter.printBase64Binary(authStr.getBytes("UTF-8"));
        //HTTP头中增加 HTTP Basic认证指示
        httpGet.setHeader("Authorization", "Basic " + encoding);

        //保存cookie。可以在一次HTTP Basic认证后，使用session维持会话，无需重复认证
        //20分钟内没有任何请求发送，则session会过期，需要重新登录
        CookieStore cookieStore = new BasicCookieStore();
        CloseableHttpClient httpClient = HttpClients.custom().setDefaultCookieStore(cookieStore).build();
        CloseableHttpResponse httpResponse = httpClient.execute(httpGet);

        LOG.info("################ Get all monitor items ################");
        printResponse(httpResponse);

        //*******************查询HDFS服务的实时监控指标************************
        //POST  https://”host”:”port”/web/monitor/service/realtime/{clusterID}/{serviceName}.do
        url = "https://" + ipAddr + ":28443/web/monitor/service/realtime/1/" + serviceName + ".do";
        HttpPost httpPost = new HttpPost(url);
        //需要查询哪些指标通过JSON串的方式携带
        httpPost.setHeader("Content-Type", "application/json;charset=UTF-8");
        //查询HDFS总存储容量
        String content = "{\"startTime\": -1,\"endTime\": -1,\"charts\": [{\"type\": \"LINE\",\"object\": {\"chartId\": \"nn_capacity_group\",\"topInfo\": false,\"seriesArray\": [{\"seriesId\": \"nn_capacitytotal\",\"deviceName\": null,\"topType\": \"0\",\"originalMetric\": null}]}}]}";
        StringEntity stringEntity = new StringEntity(content);
        stringEntity.setContentEncoding("UTF-8");
        stringEntity.setContentType("application/json");
        httpPost.setEntity(stringEntity);


        httpResponse = httpClient.execute(httpPost);

        LOG.info("################ Get total capacity ################");
        printResponse(httpResponse);
    }
}
