package com.huawei.bigdata.examples.manager;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.CookieStore;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import javax.xml.bind.DatatypeConverter;

//对应接口文档《FusionInsight V100R002C80SPC100 集群管理对外接口文档（告警）.docx》
//Fusioninsight Manager使用HTTPS协议，编写代码前需要将Manager数字证书的公钥添加到客户端代码使用的JDK的信任列表中
// 详情见《配置HTTPS证书校验》

public class Alarm {
    public static void main(String[] args) throws Exception {

        //查询告警定义，默认返回10条记录。
        // IP地址替换为对接环境的实际地址。查询参数有很多可选，具体参见接口文档
        HttpGet httpRequest = new HttpGet("https://187.7.67.8:28443/web/alarm/definitions.do");

        //HTTP Basic认证的用户名和密码，需要根据实际环境修改
        String authStr = "admin" + ":" + "Huawei!123";
        //根据协议要求，用户名密码需要使用base64编码
        String encoding = DatatypeConverter.printBase64Binary(authStr.getBytes("UTF-8"));
        //HTTP头中增加 HTTP Basic认证指示
        httpRequest.setHeader("Authorization", "Basic " + encoding);

        //保存cookie。可以在一次HTTP Basic认证后，使用session维持会话，无需重复认证
        //20分钟内没有任何请求发送，则session会过期，需要重新登录
        CookieStore cookieStore = new BasicCookieStore();
        CloseableHttpClient httpClient = HttpClients.custom().setDefaultCookieStore(cookieStore).build();

        CloseableHttpResponse httpResponse = httpClient.execute(httpRequest);

        HttpEntity entity =  httpResponse.getEntity();

        System.out.println("----------------Status Start------------------------");
        System.out.println(httpResponse.getStatusLine());
        System.out.println("---------------EntityUtils Start-------------------------");
        if (entity != null) {
            System.out.println("---------------entity is not empty-------------------------");
            //返回的告警信息为JSON串，字段的解读可以参考接口文档
            String entiryString = EntityUtils.toString(entity);
            System.out.println(entiryString);
        }
        System.out.println("----------EntityUtilsconsume Start------------------------------");
        // This ensures the connection gets released back to the manager
        EntityUtils.consume(entity);
    }
}
