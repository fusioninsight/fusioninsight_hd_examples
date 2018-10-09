package com.huawei.bigdata.examples.hdfs.rest_api;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.util.EntityUtils;
import com.huawei.fusioninsight.security.authen.spnego.client.SpnegoHttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.log4j.PropertyConfigurator;


public class CreateFile
{

    public static void main(String[] args) throws Exception
    {
        System.setProperty("java.security.auth.login.config", CreateFile.class.getClassLoader().getResource("conf/jaas.conf").getPath());// 设置指定键对值的系统属性
        System.setProperty("java.security.krb5.conf", CreateFile.class.getClassLoader().getResource("conf/krb5.conf").getPath());// 设置指定键对值的系统属性
        
        SpnegoHttpClient httpClient;
        HttpUriRequest request;
        HttpResponse response;
        HttpEntity entity;
        httpClient = new SpnegoHttpClient();
        request = new HttpGet("http://187.7.60.150:25002/webhdfs/v1/?op=LISTSTATUS");
        response = httpClient.execute(request);
        entity = response.getEntity();
        System.out.println("----------------Status Start------------------------");
        System.out.println(response.getStatusLine());
        System.out.println("---------------EntityUtils Start-------------------------");
        if (entity != null) {
            System.out.println("---------------entity is not empty-------------------------");
            String entiryString = EntityUtils.toString(entity);
            System.out.println(entiryString);
        }
        System.out.println("----------EntityUtilsconsume Start------------------------------");
        // This ensures the connection gets released back to the manager
        EntityUtils.consume(entity);
    }
}
