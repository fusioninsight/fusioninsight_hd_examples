package com.huawei.bigdata.information;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.huawei.bigdata.body.*;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AuthSchemeProvider;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.client.methods.*;
import org.apache.http.config.Lookup;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClientBuilder;


import org.apache.http.util.EntityUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import java.io.*;
import java.nio.charset.Charset;
import java.security.Principal;
import java.security.PrivilegedAction;

public class RestApi {
    private static final Logger LOG = LoggerFactory.getLogger(RestApi.class);
    public static final String URL_HTTP = "https://";
    public static HttpAuthInfo httpAuthInfo;
    private WebHcatHttpClient httpClient;
    public String name;
    public String like;
    public  Subject subject;

    //异常报错信息
    public static final String ERROR_MSG = "Json parsing error.";

    public RestApi(WebHcatHttpClient httpClient) {
        this.httpClient = httpClient;
    }
 public void login(String user,String keytab,String krb5Location){
     Kerberos kerberos = new Kerberos(user, keytab, krb5Location, false);
     LoginContext lc = kerberos.login();
     System.out.println("登录成功！！！！！");
     this.subject = lc.getSubject();
 }
    public String searchPath() {
        JSONObject rspJson = null;
        StringBuilder url = new StringBuilder();
        url.append("/templeton/v1/ddl/database");
        rspJson = sendHttpGetReq(url.toString());
        final boolean isSuccessfully = (null == rspJson ? false : rspJson.get("code").toString().contains("OK"));

        String log = String.format("[%s]: search Database %s",
                (isSuccessfully ? "SUCCESS" : "FAIL"),
                (isSuccessfully ? "successfully." : "fail."));
        System.out.println(log);
        if (!isSuccessfully) {
            System.out.println("----------------Status Start------------------------");
            System.out.println(rspJson.get("code"));
            System.out.println("---------------EntityUtils Start-------------------------");
            System.out.println("Reason for failure" + rspJson.get("error"));
            // This ensures the connection gets released back to the manager
            return null;
        }
        if (null != rspJson.get("databases")) {
            System.out.println("databases:" + rspJson.get("databases"));
            return rspJson.get("databases").toString();
        }
        return null;
    }

    public String searchPath(ReqDatabaseBody reqDatabaseBody) {
        JSONObject rspJson = null;
        StringBuilder url = new StringBuilder();
        url.append("/templeton/v1/ddl/database");
        if (reqDatabaseBody.getName() != null && reqDatabaseBody.getName().length() > 0) {
            url.append("/" + reqDatabaseBody.getName());
        }
        if (reqDatabaseBody.getLike() != null && reqDatabaseBody.getLike().length() > 0) {
            url.append("/" + reqDatabaseBody.getLike());
        }
        rspJson = sendHttpGetReq(url.toString());
        final boolean isSuccessfully = (null == rspJson ? false : rspJson.get("code").toString().contains("OK"));

        String log = String.format("[%s]: search Database %s",
                (isSuccessfully ? "SUCCESS" : "FAIL"),
                (isSuccessfully ? "successfully." : "fail."));
        System.out.println(log);
        if (!isSuccessfully) {
            System.out.println("----------------Status Start------------------------");
            System.out.println(rspJson.get("code"));
            System.out.println("---------------EntityUtils Start-------------------------");
            System.out.println("Reason for failure" + rspJson.get("error"));
            // This ensures the connection gets released back to the manager
            return null;
        }
        if (isSuccessfully) {
            System.out.println("owner:" + rspJson.get("owner"));
            System.out.println("ownerType:" + rspJson.get("ownerType"));
            System.out.println("database:" + rspJson.get("database"));
            System.out.println("code:" + rspJson.get("code"));
            System.out.println("comment:" + rspJson.get("comment"));
            System.out.println("location:" + rspJson.get("location"));
            return rspJson.get("code").toString();
        }
        return null;

    }
    public String searchTable(ReqSearchTable reqbody) {
        JSONObject rspJson = null;
        StringBuilder url = new StringBuilder();
        url.append("/templeton/v1/ddl/database");
        if (reqbody.getDataBaseName()== null) {
            System.exit(0);
            return  "DataBase can not be empty!!";

        }
        url.append("/"+reqbody.getDataBaseName()+"/table");
        if (reqbody.getLike() != null) {
            url.append("/" + like);
        }
        rspJson = sendHttpGetReq(url.toString());
        final boolean isSuccessfully = (null == rspJson ? false : rspJson.get("code").toString().contains("OK"));

        String log = String.format("[%s]: search Table %s",
                (isSuccessfully ? "SUCCESS" : "FAIL"),
                (isSuccessfully ? "successfully." : "fail."));
        System.out.println(log);
        if (!isSuccessfully) {
            System.out.println("----------------Status Start------------------------");
            System.out.println(rspJson.get("code"));
            System.out.println("---------------EntityUtils Start-------------------------");
            System.out.println("Reason for failure" + rspJson.get("error"));
            System.exit(0);
            // This ensures the connection gets released back to the manager
            return null;
        }
        if (null != rspJson.get("database")) {
            System.out.println("database:" + rspJson.get("database"));
            System.out.println("tables:" + rspJson.get("tables"));
            return rspJson.get("database").toString()+";"+rspJson.get("database");
        }
        return null;
    }
    public String SearchTableInfo(ReqTabelInfoBody reqbody) {
        JSONObject rspJson = null;
        StringBuilder url = new StringBuilder();
        url.append("/templeton/v1/ddl/database");
        if (reqbody.getDataName()== null) {
            return  "DataBase can not be empty!!";
        }
        if (reqbody.getTableName()== null) {
            return  "TaleName can not be empty!!";
        }
        url.append("/"+reqbody.getDataName()+"/table/"+reqbody.getTableName());
        if (reqbody.getFormat() != null) {
            url.append("?format=" + reqbody.getFormat());
        }
        rspJson = sendHttpGetReq(url.toString());
        final boolean isSuccessfully = (null == rspJson ? false : rspJson.get("code").toString().contains("OK"));

        String log = String.format("[%s]: Search Table Info  %s",
                (isSuccessfully ? "SUCCESS" : "FAIL"),
                (isSuccessfully ? "successfully." : "fail."));
        System.out.println(log);
        if (!isSuccessfully) {
            System.out.println("----------------Status Start------------------------");
            System.out.println(rspJson.get("code"));
            System.out.println("---------------EntityUtils Start-------------------------");
            System.out.println("Reason for failure" + rspJson.get("error"));
            System.exit(0);
            // This ensures the connection gets released back to the manager
            return null;
        }
        if (null != rspJson.get("database")) {
            System.out.println("database:" + rspJson.get("database"));
            return rspJson.get("database").toString();
        }
        return null;
    }
    public String CreateDataBase(ReqCreateDataBaBody createDataBaBody, String name) throws IOException {
        JSONObject rspJson = null;
        CloseableHttpResponse rsp = null;
        try {
            rspJson = sendHttpPutReq("/templeton/v1/ddl/database/" + name, RestHelper.toJsonString(createDataBaBody));
        } catch (JsonProcessingException e) {
            LOG.info(ERROR_MSG);
        }
        System.out.println(rspJson.get("code").toString().contains("OK"));
        final boolean isSuccessfully = (null == rspJson ? false : rspJson.get("code").toString().contains("OK"));

        String log = String.format("[%s]: Create Database  %s",
                (isSuccessfully ? "SUCCESS" : "FAIL"),
                (isSuccessfully ? "successfully." : "fail."));
        System.out.println(log);
        if (!isSuccessfully) {
            System.out.println("----------------Status Start------------------------");
            System.out.println(rspJson.get("code"));
            System.out.println("---------------EntityUtils Start-------------------------");
            System.out.println("Reason for failure" + rspJson.get("error"));
            System.exit(0);
            // This ensures the connection gets released back to the manager
            return null;
        }
        if (null != rspJson.get("database")) {
            System.out.println("database:" + rspJson.get("database"));
            return rspJson.get("database").toString();
        }
        return null;
    }
    public String CreateTable(ReqCreateTableBody reqbody, String dataBase, String table) {
        JSONObject rspJson = null;
        try{
            rspJson = sendHttpPutReq("/templeton/v1/ddl/database/" + dataBase+"/table/"+table, RestHelper.toJsonString(reqbody));
        }catch (Exception e){
            e.printStackTrace();
        }

        final boolean isSuccessfully = (null == rspJson ? false : rspJson.get("code").toString().contains("OK"));

        String log = String.format("[%s]: Create Table Info  %s",
                (isSuccessfully ? "SUCCESS" : "FAIL"),
                (isSuccessfully ? "successfully." : "fail."));
        System.out.println(log);
        if (!isSuccessfully) {
            System.out.println("----------------Status Start------------------------");
            System.out.println(rspJson.get("code"));
            System.out.println("---------------EntityUtils Start-------------------------");
            System.out.println("Reason for failure" + rspJson.get("error"));
            System.exit(0);
            // This ensures the connection gets released back to the manager
            return null;
        }
        if (null != rspJson.get("database")) {
            System.out.println("database:" + rspJson.get("database"));
            System.out.println("database:" + rspJson.get("table"));
            return rspJson.get("database").toString()+":"+rspJson.get("table");
        }
        return null;
    }
    public String CreateTable( ReqCreateDataBaBody createDataBaBody, String name) throws IOException {
        JSONObject rspJson = null;
        CloseableHttpResponse rsp = null;
        try {
            rspJson = sendHttpPutReq("/templeton/v1/ddl/database/" + name, RestHelper.toJsonString(createDataBaBody));
        } catch (JsonProcessingException e) {
            LOG.info(ERROR_MSG);
        }
        System.out.println(rspJson.get("code").toString().contains("OK"));
        final boolean isSuccessfully = (null == rspJson ? false : rspJson.get("code").toString().contains("OK"));

        String log = String.format("[%s]: Search Database Tables %s",
                (isSuccessfully ? "SUCCESS" : "FAIL"),
                (isSuccessfully ? "successfully." : "fail."));
        System.out.println(log);
        if (!isSuccessfully) {
            System.out.println("----------------Status Start------------------------");
            System.out.println(rspJson.get("code"));
            System.out.println("---------------EntityUtils Start-------------------------");
            System.out.println("Reason for failure" + rspJson.get("error"));
            System.exit(0);
            // This ensures the connection gets released back to the manager
            return null;
        }
        if (null != rspJson.get("database")) {
            System.out.println("database:" + rspJson.get("database"));
            return rspJson.get("database").toString();
        }
        return null;
    }
    public String DeleteDatabase(DeleteDataBaseBody delete) throws IOException {
        JSONObject rspJson = null;
        CloseableHttpResponse rsp = null;
        StringBuilder url = new StringBuilder();
        url.append("/templeton/v1/ddl/database/");
        if (delete.getName() != null) {
            url.append(delete.getName());
        }
        if (delete.getIfExists() != null) {
            url.append("?ifExists=" + delete.getIfExists());
        }
        if (delete.getOption() != null) {
            url.append("&&option=" + delete.getOption());
        }
        rspJson = sendHttpDelReq(url.toString());
        System.out.println(rspJson.get("code").toString().contains("OK"));
        final boolean isSuccessfully = (null == rspJson ? false : rspJson.get("code").toString().contains("OK"));

        String log = String.format("[%s]: Delete Database %s",
                (isSuccessfully ? "SUCCESS" : "FAIL"),
                (isSuccessfully ? "successfully." : "fail."));
        System.out.println(log);
        if (!isSuccessfully) {
            System.out.println("----------------Status Start------------------------");
            System.out.println(rspJson.get("code"));
            System.out.println("---------------EntityUtils Start-------------------------");
            System.out.println("Reason for failure" + rspJson.get("error"));
            System.exit(0);
            // This ensures the connection gets released back to the manager
            return null;
        }
        if (null != rspJson.get("database")) {
            System.out.println(rspJson.get("database")+" Database deleted successfully!!!!");
            return rspJson.get("database").toString();
        }
        return null;
    }
    public String DeleteTable(ReqDelTableBody delete) throws IOException {
        JSONObject rspJson = null;
        CloseableHttpResponse rsp = null;
        StringBuilder url = new StringBuilder();
        url.append("/templeton/v1/ddl/database/");
        if (delete.getTabelName() != null) {
            url.append(delete.getDataBaseName());
        }
        if (delete.getTabelName() != null) {
            url.append(delete.getTabelName());
        }
        if (delete.getIfExists() != null) {
            url.append("?ifExists=" + delete.getIfExists());
        }
        if (delete.getOption() != null) {
            url.append("&&option=" + delete.getOption());
        }
        rspJson = sendHttpDelReq(url.toString());
        System.out.println(rspJson.get("code").toString().contains("OK"));
        final boolean isSuccessfully = (null == rspJson ? false : rspJson.get("code").toString().contains("OK"));

        String log = String.format("[%s]: Delete Table %s",
                (isSuccessfully ? "SUCCESS" : "FAIL"),
                (isSuccessfully ? "successfully." : "fail."));
        System.out.println(log);
        if (!isSuccessfully) {
            System.out.println("----------------Status Start------------------------");
            System.out.println(rspJson.get("code"));
            System.out.println("---------------EntityUtils Start-------------------------");
            System.out.println("Reason for failure" + rspJson.get("error"));
            System.exit(0);
            // This ensures the connection gets released back to the manager
            return null;
        }
        if (null != rspJson.get("database")) {
            System.out.println(rspJson.get("database")+" Table deleted successfully!!!!");
            return rspJson.get("database").toString();
        }
        return null;
    }
    public String RenameTable (String dataBase,String table ,String rename) throws IOException {
        JSONObject rspJson = null;
        CloseableHttpResponse rsp = null;
            rspJson = sendHttpPost("/templeton/v1/ddl/database/"+dataBase+"/table/"+table,rename);

        System.out.println(rspJson.get("code").toString().contains("OK"));
        final boolean isSuccessfully = (null == rspJson ? false : rspJson.get("code").toString().contains("OK"));

        String log = String.format("[%s]: RenameTable Tables %s",
                (isSuccessfully ? "SUCCESS" : "FAIL"),
                (isSuccessfully ? "successfully." : "fail."));
        System.out.println(log);
        if (!isSuccessfully) {
            System.out.println("----------------Status Start------------------------");
            System.out.println(rspJson.get("code"));
            System.out.println("---------------EntityUtils Start-------------------------");
            System.out.println("Reason for failure: " + rspJson.get("statement")+rspJson.get("error"));
            System.exit(0);
            // This ensures the connection gets released back to the manager
            return null;
        }
        if (null != rspJson.get("database")) {
            System.out.println(rspJson.get("database")+" Table Rename successfully!!!!");
            return rspJson.get("database").toString();
        }
        return null;
    }
    private JSONObject  sendHttpGetReq(String uri) {
        String fullUrl = buildUrl(uri);
        HttpGet httpGet = new HttpGet(fullUrl);
        CloseableHttpResponse response = null;
        JSONObject rspJson = null;
        try {
            printHttpReqHeader(httpGet);

            response = Subject.doAs(subject, new PrivilegedAction<CloseableHttpResponse>() {
                @Override
                public CloseableHttpResponse run() {
                    HttpGet httpGet1 = new HttpGet(fullUrl);
                    try {
                        return httpClient.httpClient.execute(httpGet1);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    return null;
                }
            });
            rspJson = new JSONObject(EntityUtils.toString(response.getEntity()));
            rspJson.put("code", response.getStatusLine());
            RestHelper.checkHttpRsp(response);
            printHttpRspBody(rspJson);
        } catch (Exception e) {
            LOG.info(e.getMessage());
            return rspJson;
        } finally {
            if (null != response) {
                try {
                    response.close();
                } catch (IOException e) {
                    // nothing to do
                }
            }
        }

        return rspJson;
    }
    private JSONObject  sendHttpPost(String uri,String rename) {
        String fullUrl = buildUrl(uri);
        CloseableHttpResponse response = null;
        JSONObject rspJson = null;
        try {


            response = Subject.doAs(subject, new PrivilegedAction<CloseableHttpResponse>() {
                @Override
                public CloseableHttpResponse run() {
                    HttpPost httpPost = new HttpPost(fullUrl);
                    StringEntity entity = null;

                    try {
                        String body = "rename="+rename;
                        httpPost.setEntity(new StringEntity(body));

                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    }
                    printHttpReqHeader(httpPost);
                    try {
                        return httpClient.httpClient.execute(httpPost);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    return null;
                }
            });
            rspJson = new JSONObject(EntityUtils.toString(response.getEntity()));
            rspJson.put("code", response.getStatusLine());
            RestHelper.checkHttpRsp(response);
            printHttpRspBody(rspJson);
        } catch (Exception e) {
            LOG.info(e.getMessage());
            return rspJson;
        } finally {
            if (null != response) {
                try {
                    response.close();
                } catch (IOException e) {
                    // nothing to do
                }
            }
        }

        return rspJson;
    }
    private JSONObject sendHttpPutReq(String uri, String reqJsonStr) {
        String fullUrl = buildUrl(uri);
        CloseableHttpResponse response = null;
        JSONObject rspJson = null;

        try {
            response = Subject.doAs(subject, new PrivilegedAction<CloseableHttpResponse>() {
                @Override
                public CloseableHttpResponse run() {
                    HttpPut httpPut = new HttpPut(fullUrl);


                    try {
                        if (reqJsonStr != null && !reqJsonStr.isEmpty()) {
                            httpPut.setEntity(new StringEntity(reqJsonStr));
                        }
                        httpPut.setHeader("Content-Type", "application/json");
                        printHttpReqHeader(httpPut);
                        printHttpReqBody(reqJsonStr);
                        return httpClient.httpClient.execute(httpPut);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    return null;
                }
            });
            rspJson = new JSONObject(EntityUtils.toString(response.getEntity(), Charset.forName("UTF-8")));
            rspJson.put("code", response.getStatusLine());
            RestHelper.checkHttpRsp(response);
        } catch (Exception e) {
            LOG.info(e.getMessage());
            return rspJson;
        } finally {
            if (null != response) {
                try {
                    response.close();
                } catch (IOException e) {
                    // nothing to do
                }
            }
        }

        return rspJson;
    }

    private JSONObject sendHttpDelReq(String uri) {
        String fullUrl = buildUrl(uri);
        CloseableHttpResponse response = null;
        JSONObject rspJson = null;

        try {
            response = Subject.doAs(subject, new PrivilegedAction<CloseableHttpResponse>() {
                @Override
                public CloseableHttpResponse run() {
                    HttpDelete httpDelete = new HttpDelete(fullUrl);
                    try {

                        return httpClient.httpClient.execute(httpDelete);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    return null;
                }
            });
            rspJson = new JSONObject(EntityUtils.toString(response.getEntity(), Charset.forName("UTF-8")));
            rspJson.put("code", response.getStatusLine());
            RestHelper.checkHttpRsp(response);
        } catch (Exception e) {
            LOG.info(e.getMessage());
            return rspJson;
        } finally {
            if (null != response) {
                try {
                    response.close();
                } catch (IOException e) {
                    // nothing to do
                }
            }
        }

        return rspJson;
    }

    ;

    public static void checkHttpRsp(CloseableHttpResponse response) throws Exception {
        if (null == response) {
            throw new Exception("Http response error: response is null.");
        }

        if (HttpStatus.SC_OK != response.getStatusLine().getStatusCode()) {
            throw new Exception("Http response error: " + response.getStatusLine()
                    + "\n " + inputStreamToStr(response.getEntity().getContent()));
        }
    }

    public static String inputStreamToStr(InputStream in) {
        if (null == in) {
            return null;
        }

        StringBuffer strBuf = new StringBuffer("");
        BufferedReader bufferedReader = null;

        try {
            bufferedReader = new BufferedReader(new InputStreamReader(in));
            String lineContent = bufferedReader.readLine();
            while (lineContent != null) {
                strBuf.append(lineContent);
                lineContent = bufferedReader.readLine();
            }
        } catch (IOException e) {
            System.out.println("Exception: " + e.getMessage());
        } finally {
            if (null != bufferedReader) {
                try {
                    bufferedReader.close();
                } catch (IOException ignore) {
                    // to do nothing.
                }
            }
        }
        return strBuf.toString();
    }

    public void printHttpReqHeader(HttpUriRequest httpUriRequest) {
        System.out.println("REQ HEADER: " + httpUriRequest.getRequestLine());
    }

    public void printHttpReqBody(JSONObject reqJson) {
        System.out.println("REQ BODY:   " + (null == reqJson ? "null" : reqJson.toString(4)));
    }

    public void printHttpReqBody(String reqJsonStr) {
        System.out.println("REQ BODY:   " + (null == reqJsonStr ? "null" : reqJsonStr));
    }

    public void printHttpRspBody(JSONObject rspJson) {
        System.out.println("RSP BODY:   " + (null == rspJson ? "null" : rspJson.toString(4)));
    }

    private String buildUrl(String partUrl) {
        StringBuilder sb = new StringBuilder();
        sb.append(URL_HTTP)
                .append(this.httpClient.httpAuthInfo.getIp()).append(":").append(this.httpClient.httpAuthInfo.getPort())
                .append(partUrl);
        String fullUrl = sb.toString();
        return fullUrl;
    }

    private static HttpClientBuilder getClient() {
        HttpClientBuilder builder = HttpClientBuilder.create();
        Lookup<AuthSchemeProvider> authSchemeRegistry = RegistryBuilder.<AuthSchemeProvider>create().
                register(AuthSchemes.SPNEGO, new SPNegoSchemeFactory(true)).build();
        builder.setDefaultAuthSchemeRegistry(authSchemeRegistry);
        BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(new AuthScope(null, -1, null), new Credentials() {
            @Override
            public Principal getUserPrincipal() {
                return null;
            }

            @Override
            public String getPassword() {
                return null;
            }
        });
        builder.setDefaultCredentialsProvider(credentialsProvider);
        return builder;
    }
}
