package com.huawei.bigdata.examples.manager;

import org.apache.http.HttpEntity;
import org.apache.http.client.CookieStore;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.CharArrayBuffer;
import org.apache.http.util.EntityUtils;

import javax.xml.bind.DatatypeConverter;
import java.io.*;

//1.因为Manager使用的是HTTPS协议，需要参考《配置HTTPS证书校验》将服务端证书添加到客户端的信任列表中
//2.请参考《FusionInsight V100R002C70 集群管理对外接口文档（配置）.docx》 2.1	生成客户端配置 2.2	下载客户端配置 章节接口介绍
//   A.通知后台生成客户端及其配置文件。可以通过参数指定服务:URL	POST  https://”host”:”port”/web/config/export/clientfiles/{clusterID}.do
//   B.下载生成的客户端文件（压缩包）。URL	GET  https://”host”:”port”/web/config/download/clientfiles.do

public class DownloadClient {
    public static void main(String[] args) throws IOException {
        String IP ="189.211.69.122";
        String port ="28443";
        CookieStore cookieStore = new BasicCookieStore();
        CloseableHttpClient httpClient = HttpClients.custom().setDefaultCookieStore(cookieStore).build();
        String url2 ="https://"+IP+":"+port+"/web/config/export/clientfiles/1.do";
        HttpPost httpPost = new HttpPost(url2);
        String auth = "admin" + ":" + "Huawei@123";
        String s = DatatypeConverter.printBase64Binary(auth.getBytes("UTF-8"));

        httpPost.setHeader("Authorization", "Basic " + s);
//        CloseableHttpResponse execute = httpClient.execute(httpPost);
////        System.out.println(execute);
        httpPost.setHeader("Content-Type", "application/json;charset=UTF-8");
        //serviceNames:以数组形式给出所需要的服务。如果是空数组，则下载所有服务的客户端。
        String content = "{\"onlyConfig\": false, \"onlyCreate\": false, \"clientPath\": \"/tmp/FusionInsight-Client/\", \"serviceNames\": [\"Flink\"]}";
        StringEntity stringEntity = new StringEntity(content);
        stringEntity.setContentEncoding("UTF-8");
        stringEntity.setContentType("application/json");
        httpPost.setEntity(stringEntity);
        CloseableHttpResponse execute1 = httpClient.execute(httpPost);
        HttpEntity entity = execute1.getEntity();
        String s1 = EntityUtils.toString(entity);
        System.out.println(s1);

        String url1 ="https://"+IP+":"+port+"/web/config/download/clientfiles.do";
        HttpGet httpGet = new HttpGet(url1);
        CloseableHttpResponse execute = httpClient.execute(httpGet);
        HttpEntity entity1 = execute.getEntity();

        getFile(entity1.getContent(),"D:\\222.zip");
    }

    public static void getFile(InputStream is,String fileName) throws IOException{
        BufferedInputStream in=null;
        BufferedOutputStream out=null;
        in=new BufferedInputStream(is);
        out=new BufferedOutputStream(new FileOutputStream(fileName));
        int len=-1;
        byte[] b=new byte[1024];
        while((len=in.read(b))!=-1){
            out.write(b,0,len);
        }
        in.close();
        out.close();
    }

}
