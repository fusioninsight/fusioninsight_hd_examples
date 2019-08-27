package testjar;

import com.huawei.hadoop.om.rest.exception.*;
import com.huawei.hadoop.om.rest.login.BigdataSslSocketFactory;
import com.huawei.hadoop.om.rest.validutil.ParamsValidUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.http.Consts;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.entity.mime.content.StringBody;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.*;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;

public class CasLogin
{
    private static final Logger LOG = LoggerFactory.getLogger(com.huawei.hadoop.om.rest.login.CasLogin.class);
    
    private static final String SET_COOKIE = "Set-Cookie";
    
    private static final String CAS_SESSION_ID_STRING = "casSessionId=";
    
    private static final String CAS_TGC_STRING = "CASTGC=";
    
    private static final String SEMICOLON_SEPARATOR = ";";
    
    private static final String DOUBLE_QUOTAYION = "\"";
    
    private static final String LT_STRING = "name=\"lt\" value=";
    
    private static final int LT_INDEX = 5;
    
    private static final String POSTRESPONSE_STRING = "\"id\":-1,\"state\":\"";
    
    private static final String POSTRESPONSE_COMPLETE = "COMPLETE";
    
    private static final String CREDENTIALS_WRONG = "The credentials you provided cannot be determined to be authentic";
    
    private static final String RESETPASSWORD = "modify_password.html";
    
    private static final String PROTOCOL_NAME = "https";
    
    private static final int PORT = 443;
    
    private static final String DEFAULT_SSL_VERSION = "TLSv1.1";
    
    private static final String DEFAULT_PASSWORD = "Admin@123";
    
    private CasLogin()
    {
        
    }
    
    public static HttpClient usernamePasswordLogin(String casUrl, String webUrl, String userName, String password, String userTLSVersion)
        throws InvalidInputParamException, GetCasLoginPageException, LoginCasServerException, FirstTimeLoginException,
            WrongUsernameOrPasswordException, WebLoginCheckException
    {
        if (ParamsValidUtil.isEmpty(casUrl, webUrl, userName, password))
        {
            LOG.error("Invalid input param.");
            throw new InvalidInputParamException("Invalid input param.");
            
        }
        
        String keytabFilePath = "";
        
        HttpClient httpClient = login(casUrl, webUrl, userName, password, userTLSVersion, keytabFilePath);
        
        if (httpClient == null)
        {
            LOG.error("The httpClient is empty,login failed.");
            return null;
        }
        
        return httpClient;
    }
    
    public static HttpClient usernamePasswordFirstLogin(String casUrl, String webUrl, String userName, String password, String userTLSVersion)
        throws InvalidInputParamException, GetCasLoginPageException, LoginCasServerException, FirstTimeLoginException,
            WrongUsernameOrPasswordException, WebLoginCheckException
    {
        if (ParamsValidUtil.isEmpty(casUrl, webUrl, userName, password))
        {
            LOG.error("Invalid input param.");
            throw new InvalidInputParamException("Invalid input param.");
            
        }
        
        String keytabFilePath = "";
        
        HttpClient httpClient = firstlogin(casUrl, webUrl, userName, password, userTLSVersion, keytabFilePath);
        
        if (httpClient == null)
        {
            LOG.error("The httpClient is empty,login failed.");
            return null;
        }
        
        return httpClient;
    }
    
    public static HttpClient usernameKeytabLogin(String casUrl, String webUrl, String userName, String keytabFilePath)
        throws InvalidInputParamException, GetCasLoginPageException, LoginCasServerException, FirstTimeLoginException,
            WrongUsernameOrPasswordException, WebLoginCheckException
    {
        LOG.info("Enter login with keytab.");
        if (ParamsValidUtil.isEmpty(casUrl, webUrl, userName, keytabFilePath))
        {
            LOG.error("Invalid input param.");
            throw new InvalidInputParamException("Invalid input param.");
        }
        
        File keytabFile = new File(keytabFilePath);
        if (!keytabFile.exists())
        {
            LOG.warn("The keytabFile does not exist.");
            return null;
        }
        
        String userTLSVersion = DEFAULT_SSL_VERSION;
        String password = DEFAULT_PASSWORD;
        
        HttpClient httpClient = login(casUrl, webUrl, userName, password, userTLSVersion, keytabFilePath);
        
        if (httpClient == null)
        {
            LOG.error("The httpClient is empty,login failed.");
            return null;
        }
        
        return httpClient;
        
    }
    
    public static HttpClient oldJdkPasswordLogin(String casUrl, String webUrl, String userName, String password,
                                                 String userTLSVersion)
        throws InvalidInputParamException, GetCasLoginPageException, LoginCasServerException, FirstTimeLoginException,
            WrongUsernameOrPasswordException, WebLoginCheckException
    {
        if (ParamsValidUtil.isEmpty(casUrl, webUrl, userName, password, userTLSVersion))
        {
            LOG.error("Invalid input param.");
            throw new InvalidInputParamException("Invalid input param.");
            
        }
        
        String keytabFilePath = "";
        HttpClient httpClient = login(casUrl, webUrl, userName, password, userTLSVersion, keytabFilePath);
        
        if (httpClient == null)
        {
            LOG.error("The httpClient is empty,login failed.");
            return null;
        }
        
        return httpClient;
    }
    
    public static HttpClient oldJdkKeytabLogin(String casUrl, String webUrl, String userName, String keytabFilePath,
                                               String userTLSVersion)
        throws InvalidInputParamException, GetCasLoginPageException, LoginCasServerException, FirstTimeLoginException,
            WrongUsernameOrPasswordException, WebLoginCheckException
    {
        if (ParamsValidUtil.isEmpty(casUrl, webUrl, userName, keytabFilePath, userTLSVersion))
        {
            LOG.error("Invalid input param.");
            throw new InvalidInputParamException("Invalid input param.");
        }
        
        String password = DEFAULT_PASSWORD;
        HttpClient httpClient = login(casUrl, webUrl, userName, password, userTLSVersion, keytabFilePath);
        
        if (httpClient == null)
        {
            LOG.error("The httpClient is empty,login failed.");
            return null;
        }
        
        return httpClient;
    }
    
    private static HttpClient firstlogin(String casUrl, String webUrl, String userName, String password,
                                         String userTLSVersion, String keytabFilePath)
        throws InvalidInputParamException, GetCasLoginPageException, LoginCasServerException, FirstTimeLoginException,
            WrongUsernameOrPasswordException, WebLoginCheckException
    {
        LOG.info("Enter login.");
        if (ParamsValidUtil.isEmpty(casUrl, webUrl, userName))
        {
            LOG.error("Invalid input param.");
            throw new InvalidInputParamException("Invalid input param.");
        }
        
        if (ParamsValidUtil.isEmpty(userTLSVersion))
        {
            userTLSVersion = DEFAULT_SSL_VERSION;
        }
        
        if (ParamsValidUtil.isEmpty(password))
        {
            password = DEFAULT_PASSWORD;
        }
        
        LOG.info("1. Get http client for sending https request.");
        //获取httpClient
        HttpClient httpClient = getHttpClient(userTLSVersion);
        if (ParamsValidUtil.isNull(httpClient))
        {
            LOG.error("Get http client error.");
            throw new GetCasLoginPageException("Get http client error.");
        }
        
        //获取CAS登录页面
        LOG.info("2. Get cas login page.");
        HttpResponse casLoginPageResponse = getCasLoginPage(casUrl, httpClient);
        if (ParamsValidUtil.isNull(casLoginPageResponse))
        {
            LOG.error("Get cas login page error.");
            throw new GetCasLoginPageException("Get cas login page error.");
        }
        LOG.info("Get cas login page request's status is: {}.", casLoginPageResponse.getStatusLine());
        
        //获取sessionId 和loginTicket
        LOG.info("3. Get cas session id and login ticket from cas login page.");
        String casSessionId = getCasSessionId(casLoginPageResponse);
        String lt = getLoginTicket(casLoginPageResponse);
        if (ParamsValidUtil.isEmpty(casSessionId, lt))
        {
            LOG.error("The cas session id  or login ticket is null.");
            throw new GetCasLoginPageException("The cas session id  or login ticket is null.");
        }
        
        //到cas进行登录认证
        LOG.info("4. Send cas login POST request, usename is {}.", userName);
        String postUrl = generateCasLoginUrl(casUrl, webUrl);
        HttpResponse loginPostResponse =
            loginCasServer(postUrl, userName, password, keytabFilePath, httpClient, casSessionId, lt);
        
        if (ParamsValidUtil.isNull(loginPostResponse))
        {
            LOG.error("Post cas login response error.");
            throw new LoginCasServerException("Post cas login response error.");
        }
        LOG.info("Send cas login POST request's status is: {}. ", loginPostResponse.getStatusLine());
        
        //登录检查
        LOG.info("6. Send web login check request, username is {}.", userName);
        HttpResponse webLoginCheck = webLoginCheckFirstLogin(webUrl, httpClient);
        
        if (ParamsValidUtil.isNull(webLoginCheck))
        {
            LOG.error("Get web login check error.");
            throw new WebLoginCheckException("Get web login check error.");
        }
        LOG.info("Send web login check request's status is: {}.", webLoginCheck.getStatusLine());
        
        //登录认证成功，则返回可用的httpClient
        return httpClient;
    }
    
    private static HttpClient login(String casUrl, String webUrl, String userName, String password,
                                    String userTLSVersion, String keytabFilePath)
        throws InvalidInputParamException, GetCasLoginPageException, LoginCasServerException, FirstTimeLoginException,
            WrongUsernameOrPasswordException, WebLoginCheckException
    {
        LOG.info("Enter login.");
        if (ParamsValidUtil.isEmpty(casUrl, webUrl, userName))
        {
            LOG.error("Invalid input param.");
            throw new InvalidInputParamException("Invalid input param.");
        }
        
        if (ParamsValidUtil.isEmpty(userTLSVersion))
        {
            userTLSVersion = DEFAULT_SSL_VERSION;
        }
        
        if (ParamsValidUtil.isEmpty(password))
        {
            password = DEFAULT_PASSWORD;
        }
        
        LOG.info("1. Get http client for sending https request.");
        //获取httpClient
        HttpClient httpClient = getHttpClient(userTLSVersion);
        if (ParamsValidUtil.isNull(httpClient))
        {
            LOG.error("Get http client error.");
            throw new GetCasLoginPageException("Get http client error.");
        }
        
        //获取CAS登录页面
        LOG.info("2. Get cas login page.");
        HttpResponse casLoginPageResponse = getCasLoginPage(casUrl, httpClient);
        if (ParamsValidUtil.isNull(casLoginPageResponse))
        {
            LOG.error("Get cas login page error.");
            throw new GetCasLoginPageException("Get cas login page error.");
        }
        LOG.info("Get cas login page request's status is: {}.", casLoginPageResponse.getStatusLine());
        
        //获取sessionId 和loginTicket
        LOG.info("3. Get cas session id and login ticket from cas login page.");
        String casSessionId = getCasSessionId(casLoginPageResponse);
        String lt = getLoginTicket(casLoginPageResponse);
        if (ParamsValidUtil.isEmpty(casSessionId, lt))
        {
            LOG.error("The cas session id  or login ticket is null.");
            throw new GetCasLoginPageException("The cas session id  or login ticket is null.");
        }
        
        //到cas进行登录认证
        LOG.info("4. Send cas login POST request, usename is {}.", userName);
        String postUrl = generateCasLoginUrl(casUrl, webUrl);
        HttpResponse loginPostResponse =
            loginCasServer(postUrl, userName, password, keytabFilePath, httpClient, casSessionId, lt);
        
        if (ParamsValidUtil.isNull(loginPostResponse))
        {
            LOG.error("Post cas login response error.");
            throw new LoginCasServerException("Post cas login response error.");
        }
        LOG.info("Send cas login POST request's status is: {}. ", loginPostResponse.getStatusLine());
        
        //获取认证后的TGC
        LOG.info("5. Get CASTGC from cas login POST response, username is {}.", userName);
        String castgc = getCASTGC(loginPostResponse);
        
        if (ParamsValidUtil.isEmpty(castgc))
        {
            LOG.error("The cas tgc is null.");
            throw new LoginCasServerException("The cas tgc is null.");
        }
        
        //登录检查
        LOG.info("6. Send web login check request, username is {}.", userName);
        HttpResponse webLoginCheck = webLoginCheck(webUrl, httpClient);
        
        if (ParamsValidUtil.isNull(webLoginCheck))
        {
            LOG.error("Get web login check error.");
            throw new WebLoginCheckException("Get web login check error.");
        }
        LOG.info("Send web login check request's status is: {}.", webLoginCheck.getStatusLine());
        
        //登录认证成功，则返回可用的httpClient
        return httpClient;
    }
    
    /**
     * 发起web应用的登陆校验请求
     * 
     * @param webUrl
     *            web 应用url
     * @param httpclient
     *            http client
     * @return response
     *            登陆校验的响应对象
     * @throws LoginCasServerException
     *            自定义cas认证异常
     *         
     */
    private static HttpResponse webLoginCheckFirstLogin(String webUrl, HttpClient httpclient)
        throws LoginCasServerException
    {
        LOG.info("Enter webLoginCheck.");
        HttpGet loginCheckHttpGet = new HttpGet(webUrl + "/access/login_check.htm");
        
        HttpResponse response = null;
        BufferedReader bufferedReader = null;
        InputStream inputStream = null;
        boolean flag = false;
        try
        {
            response = httpclient.execute(loginCheckHttpGet);
            inputStream = response.getEntity().getContent();
            bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            String lineContent = "";
            lineContent = bufferedReader.readLine();
            
            LOG.info("webLoginCheck response content is {}.", lineContent);
            
            flag = true;
            
                        
        }
        catch (UnsupportedEncodingException e)
        {
            LOG.error("Get lt failed because of UnsupportedEncodingException.");
        }
        catch (ClientProtocolException e)
        {
            LOG.error("Get lt failed because of ClientProtocolException.");
        }
        catch (IOException e)
        {
            LOG.error("Get lt failed because of IOException.");
        }
        catch (IllegalStateException e)
        {
            LOG.error("Get lt failed because of IllegalStateException.");
        }
        finally
        {
            if (bufferedReader != null)
            {
                try
                {
                    bufferedReader.close();
                }
                catch (IOException e)
                {
                    LOG.warn("Close bufferedReader failed.");
                }
            }
            if (inputStream != null)
            {
                try
                {
                    inputStream.close();
                }
                catch (IOException e)
                {
                    LOG.warn("Close inputStream failed.");
                }
                
            }
            
        }
        
        if (!flag)
        {
            throw new LoginCasServerException("webLoginCheck failed ");
        }
        return response;
    }
    
    /**
     * 发起web应用的登陆校验请求
     * 
     * @param webUrl
     *            web 应用url
     * @param httpclient
     *            http client
     * @return response
     *            登陆校验的响应对象
     * @throws LoginCasServerException
     *            自定义cas认证异常
     *         
     */
    private static HttpResponse webLoginCheck(String webUrl, HttpClient httpclient)
        throws LoginCasServerException
    {
        LOG.info("Enter webLoginCheck.");
        HttpGet loginCheckHttpGet = new HttpGet(webUrl + "/access/login_check.htm");
        
        HttpResponse response = null;
        BufferedReader bufferedReader = null;
        InputStream inputStream = null;
        boolean flag = false;
        try
        {
            response = httpclient.execute(loginCheckHttpGet);
            inputStream = response.getEntity().getContent();
            bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            String lineContent = "";
            lineContent = bufferedReader.readLine();
            
            LOG.info("webLoginCheck response content is {}.", lineContent);
            String postResponseState = "";
            
            while (lineContent != null)
            {
                LOG.debug("lineContent={}", lineContent);
                
                //根据页面返回的成功或失败描述，进行相应处理。                
                if ((null != lineContent) && (lineContent.contains(POSTRESPONSE_STRING)))
                {
                    lineContent = lineContent.trim();
                    
                    String temp = lineContent.substring(POSTRESPONSE_STRING.length() + 1);
                    
                    LOG.debug("temp={}", temp);
                    
                    postResponseState = temp.substring(0, temp.indexOf(DOUBLE_QUOTAYION));
                    
                    LOG.debug("postResponseState={}", postResponseState);
                    if (postResponseState.equals(POSTRESPONSE_COMPLETE))
                    {
                        flag = true;
                        LOG.debug("webLoginCheck complete");
                        break;
                    }
                }
                lineContent = bufferedReader.readLine();
            }
            
        }
        catch (UnsupportedEncodingException e)
        {
            LOG.error("Get lt failed because of UnsupportedEncodingException.");
        }
        catch (ClientProtocolException e)
        {
            LOG.error("Get lt failed because of ClientProtocolException.");
        }
        catch (IOException e)
        {
            LOG.error("Get lt failed because of IOException.");
        }
        catch (IllegalStateException e)
        {
            LOG.error("Get lt failed because of IllegalStateException.");
        }
        finally
        {
            if (bufferedReader != null)
            {
                try
                {
                    bufferedReader.close();
                }
                catch (IOException e)
                {
                    LOG.warn("Close bufferedReader failed.");
                }
            }
            if (inputStream != null)
            {
                try
                {
                    inputStream.close();
                }
                catch (IOException e)
                {
                    LOG.warn("Close inputStream failed.");
                }
                
            }
            
        }
        
        if (!flag)
        {
            throw new LoginCasServerException("webLoginCheck failed ");
        }
        return response;
    }
    
    /**
     * 从登录CAS Server的响应中，获取TGC
     * 
     * @param loginPostResponse
     *            响应对象
     * @return TGC
     *            认证后的票据
     */
    private static String getCASTGC(HttpResponse loginPostResponse)
    {
        LOG.info("Enter getCASTGC.");
        Header header = loginPostResponse.getLastHeader(SET_COOKIE);
        String casTgcHeader = header == null ? "" : header.getValue();
        String tempCasTgc = casTgcHeader.split(SEMICOLON_SEPARATOR)[0];
        if (StringUtils.isEmpty(tempCasTgc))
        {
            LOG.info("The tempCasTgc is empty.");
            return null;
        }
        
        String casTgc = tempCasTgc.substring(CAS_TGC_STRING.length());
        LOG.info("Exit getCASTGC.");
        return casTgc;
    }
    
    /**
     * 生成登录CAS的完整URL
     * @param casUrl
     * @param webUrl
     * @return
     */
    private static String generateCasLoginUrl(String casUrl, String webUrl)
    {
        LOG.info("Enter generateCasLoginUrl");
        StringBuilder sb = new StringBuilder();
        sb.append(casUrl);
        sb.append("?service=");
        sb.append(webUrl);
        sb.append("/cas_security_check.htm");
        
        LOG.info("Exit generateCasLoginUrl");
        return sb.toString();
    }
    
    /**
     * 从返回的登录界面响应中，获取login ticket
     * 
     * @param preLoginResponse
     *            登陆页面请求响应
     * @return lt
     *            登录认证票据
     */
    private static String getLoginTicket(HttpResponse preLoginResponse)
    {
        LOG.info("Enter getLoginTicket.");
        BufferedReader bufferedReader = null;
        InputStream inputStream = null;
        String loginTicket = "";
        try
        {
            inputStream = preLoginResponse.getEntity().getContent();
            
            // 当http response不为空时，其context属性不会为null
            bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            String lineContent = "";
            while (null != lineContent)
            {
                try
                {
                    lineContent = bufferedReader.readLine();
                }
                catch (IOException e)
                {
                    LOG.error("Failed to read a line.", e);
                    return loginTicket;
                }
                
                if ((null != lineContent) && (lineContent.contains(LT_STRING)))
                {
                    //去掉该行头尾的空格
                    lineContent = lineContent.trim();
                    // 如果LT_STRING(即 name=\"loginTicket\" value=)存在，则根据双引号分割执行split操作，数组长度不会小于6
                    loginTicket = lineContent.split(DOUBLE_QUOTAYION)[LT_INDEX];
                    LOG.info("The loginTicket is {}.", loginTicket);
                    break;
                }
                
            }
        }
        catch (IllegalStateException e)
        {
            LOG.error("Get lt failed because of IllegalStateException.");
        }
        catch (IOException e)
        {
            LOG.error("Get lt failed because of IOException.");
        }
        finally
        {
            
            if (bufferedReader != null)
            {
                try
                {
                    bufferedReader.close();
                }
                catch (IOException e)
                {
                    LOG.warn("Close buffer reader failed.");
                }
                
            }
            
            if (inputStream != null)
            {
                try
                {
                    inputStream.close();
                }
                catch (IOException e)
                {
                    LOG.warn("Close buffer reader failed.");
                }
                
            }
            
        }
        LOG.info("Exit getLoginTicket.");
        return loginTicket;
    }
    
    /**
     * 从返回的登录界面响应中，获取CAS session id
     * 
     * @param preLoginResponse
     *            登陆页面请求响应
     * @return session id
     *            会话ID
     */
    private static String getCasSessionId(HttpResponse preLoginResponse)
    {
        LOG.info("Enter getCasSessionId.");
        Header resHeader = preLoginResponse.getFirstHeader(SET_COOKIE);
        String setCookie = resHeader == null ? "" : resHeader.getValue();
        
        String casSessionId =
            setCookie.substring(CAS_SESSION_ID_STRING.length(), setCookie.indexOf(SEMICOLON_SEPARATOR));
        LOG.info("The casSessionId is .", casSessionId);
        
        LOG.info("Exit getCasSessionId.");
        return casSessionId;
    }
    
    /**
     * 获取CAS登录界面
     * 
     * @param casUrl
     *            cas首页地址
     * @param httpclient
     *            http client
     * @return response
     *            请求的响应
     */
    private static HttpResponse getCasLoginPage(String casUrl, HttpClient httpclient)
    {
        LOG.info("Enter getCasLoginPage.");
        LOG.info("the casUrl is:{}", casUrl);
        HttpGet preLoginHttpGet = new HttpGet(casUrl);
        HttpResponse response = null;
        try
        {
            response = httpclient.execute(preLoginHttpGet);
        }
        catch (ClientProtocolException e)
        {
            LOG.error("Execute cas login failed because of ClientProtocolException.");
        }
        catch (IOException e)
        {
            //qqq=================
            e.printStackTrace();
            LOG.error("Execute cas login failed because of IOException.{}", e.getMessage());
        }
        
        return response;
    }
    
    /**
     * 获得http client
     * 
     * @return http client
     */
    public static HttpClient getHttpClient(String userTLSVersion)
    {
        LOG.info("Enter getHttpClient.");       
        SSLContext sslContext = null;
        
        try
        {
            sslContext = SSLContext.getInstance(userTLSVersion);
            
            if (sslContext == null)
            {
                return null;
            }
            
            LOG.info("SSLContext.getInstance finish.");
        }
        catch (NoSuchAlgorithmException e1)
        {
            e1.printStackTrace();
            return null;
        }
        
        X509TrustManager trustManager = new X509TrustManager()
        {
            
            public X509Certificate[] getAcceptedIssuers()
            {
                return null;
            }
            
            public void checkClientTrusted(X509Certificate ax509certificate[], String s)
                throws CertificateException
            {
            }
            
            public void checkServerTrusted(X509Certificate ax509certificate[], String s)
                throws CertificateException
            {
            }
            
        };
        try
        {
            sslContext.init(null, new TrustManager[] {trustManager}, new SecureRandom());
        }
        catch (KeyManagementException e)
        {
            e.printStackTrace();
            return null;
        }
        SSLSocketFactory sslSocketFactory = null;
        if (ParamsValidUtil.isEmpty(userTLSVersion) || userTLSVersion.equals(DEFAULT_SSL_VERSION))
        {
            sslSocketFactory = new SSLSocketFactory(sslContext, SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
        }
        else
        {
            sslSocketFactory =
                new BigdataSslSocketFactory(sslContext, SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER, userTLSVersion);
        }
        
        HttpClient httpClient = new DefaultHttpClient();
        ClientConnectionManager connectionManager = httpClient.getConnectionManager();
        SchemeRegistry schemeRegistry = connectionManager.getSchemeRegistry();
        schemeRegistry.register(new Scheme(PROTOCOL_NAME, PORT, sslSocketFactory));
        return new DefaultHttpClient(connectionManager, httpClient.getParams());
        
    }
    
    private static HttpResponse loginCasServer(String postUrl, String userName, String password, String keytabFilePath,
                                               HttpClient httpClient, String casSessionId, String lt)
        throws LoginCasServerException, InvalidInputParamException, GetCasLoginPageException, LoginCasServerException,
            FirstTimeLoginException, WrongUsernameOrPasswordException
    {
        LOG.info("Enter loginCasServer.");
        if (ParamsValidUtil.isEmpty(postUrl, userName, password, casSessionId, lt))
        {
            LOG.error("Invalid input param.");
            throw new InvalidInputParamException("Invalid input param.");
        }
        
        LOG.info("The postUrl is {}", postUrl);
        HttpPost httpPost = new HttpPost(postUrl);
        HttpEntity reqEntity = null;
        
        List<BasicNameValuePair> FormData = null;
        
        if (!ParamsValidUtil.isEmpty(keytabFilePath))
        {
            LOG.debug("The keytab file path is {}.", keytabFilePath);
            FileBody bin = new FileBody(new File(keytabFilePath));
            reqEntity =
                MultipartEntityBuilder.create()
                    .addPart("keytab", bin)
                    .addPart("username", new StringBody(userName, ContentType.create("text/plain", Consts.UTF_8)))
                    .addPart("password", new StringBody(password, ContentType.create("text/plain", Consts.UTF_8)))
                    .addPart("lt", new StringBody(lt, ContentType.create("text/plain", Consts.UTF_8)))
                    .addPart("_eventId", new StringBody("submit", ContentType.create("text/plain", Consts.UTF_8)))
                    .addPart("submit", new StringBody("Login", ContentType.create("text/plain", Consts.UTF_8)))
                    .build();
            
        }
        else
        {
            /*reqEntity =
                MultipartEntityBuilder.create()
                    .addPart("username", new StringBody(userName, ContentType.create("text/plain", Consts.UTF_8)))
                    .addPart("password", new StringBody(password, ContentType.create("text/plain", Consts.UTF_8)))
                    .addPart("lt", new StringBody(lt, ContentType.create("text/plain", Consts.UTF_8)))
                    .addPart("_eventId", new StringBody("submit", ContentType.create("text/plain", Consts.UTF_8)))
                    .addPart("submit", new StringBody("Login", ContentType.create("text/plain", Consts.UTF_8)))
                    .build();*/
            
            FormData = new ArrayList<BasicNameValuePair>();
            FormData.add(new BasicNameValuePair("username", userName));
            FormData.add(new BasicNameValuePair("password", password));
            FormData.add(new BasicNameValuePair("lt", lt));
            FormData.add(new BasicNameValuePair("_eventId", "submit"));
            FormData.add(new BasicNameValuePair("submit", "Login"));
            
        }
        
        HttpResponse response = null;
        BufferedReader bufferedReader = null;
        InputStream inputStream = null;
        
        try
        {
            httpPost.addHeader("Cookie", CAS_SESSION_ID_STRING + casSessionId);
            //httpPost.setEntity(reqEntity);
            httpPost.setEntity(new UrlEncodedFormEntity(FormData, "UTF-8"));
            response = httpClient.execute(httpPost);
            
            // 根据页面返回的提示信息，进行相应处理,Java方式目前CAS登录页面返回结果只有两种异常分支。  
            LOG.info("Login CasServer status is {}", response.getStatusLine());
            inputStream = response.getEntity().getContent();
            bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            String lineContent = bufferedReader.readLine();
            while (lineContent != null)
            {
                LOG.info("lineContent={}", lineContent);
                
                // 用户名或密码错误的异常分支
                if (lineContent.contains(CREDENTIALS_WRONG))
                {
                    LOG.error("The credentials you provided cannot be determined to be authentic.");
                    throw new WrongUsernameOrPasswordException(
                        "The credentials you provided cannot be determined to be authentic.");
                }
                // 首次登录，重置密码的异常分支
                if (lineContent.contains(RESETPASSWORD))
                {
                    LOG.warn("Login first time,please reset password.");
//                    throw new FirstTimeLoginException("Login first time ,please reset password");
                    return response;
                }
                lineContent = bufferedReader.readLine();
            }
        }
        catch (UnsupportedEncodingException e)
        {
            LOG.error("Login cas server failed because of UnsupportedEncodingException.");
            throw new LoginCasServerException("UnsupportedEncodingException");
            
        }
        catch (ClientProtocolException e)
        {
            LOG.error("Login cas server failed because of LoginCasServerException.");
            throw new LoginCasServerException("ClientProtocolException");
        }
        catch (IOException e)
        {
            LOG.error("Login cas server failed because of ClientProtocolException.");
            throw new LoginCasServerException("IOException");
        }
        catch (IllegalStateException e)
        {
            LOG.error("Login cas server failed because of IllegalStateException.");
            throw new LoginCasServerException("IllegalStateException");
        }
        
        finally
        {
            if (bufferedReader != null)
            {
                try
                {
                    bufferedReader.close();
                }
                catch (IOException e)
                {
                    LOG.warn("Close buffer reader failed.");
                }
            }
            
            if (inputStream != null)
            {
                try
                {
                    inputStream.close();
                }
                catch (IOException e)
                {
                    LOG.warn("Close buffer reader failed.");
                }
            }
        }
        return response;
    }
}
