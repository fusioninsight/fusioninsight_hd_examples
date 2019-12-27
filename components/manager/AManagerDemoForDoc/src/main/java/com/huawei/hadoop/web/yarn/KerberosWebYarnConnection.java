package com.huawei.hadoop.web.yarn;


import com.google.gson.Gson;
import com.huawei.hadoop.webhdfs.client.HttpsHostnameVerifier;
import com.huawei.hadoop.webhdfs.client.HttpsX509TrustManager;
import com.huawei.hadoop.webhdfs.client.KerberosAuthenticator;
import com.huawei.hadoop.webhdfs.client.URLUtil;
import com.huawei.hadoop.webhdfs.client.WebHDFSConnection;

import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL.Token;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.X509TrustManager;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.text.MessageFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class KerberosWebYarnConnection implements WebHDFSConnection {

    protected static final Logger Log = LoggerFactory.getLogger(KerberosWebYarnConnection.class);

    private String httpYarnUrl = null;
    private Token token = null;
    private AuthenticatedURL authenticatedURL = null;

    static {
        SSLContext sslContext = null;
        try {
            sslContext = SSLContext.getInstance("TLS");
            X509TrustManager[] xtmArray = new X509TrustManager[]{new HttpsX509TrustManager()};
            sslContext.init(null, xtmArray, new java.security.SecureRandom());
        } catch (GeneralSecurityException e) {
            e.printStackTrace();
        }

        if (sslContext != null) {
            HttpsURLConnection.setDefaultSSLSocketFactory(sslContext.getSocketFactory());
        }

        HttpsURLConnection.setDefaultHostnameVerifier(new HttpsHostnameVerifier());
    }

    public KerberosWebYarnConnection(String httpYarnUrl, String krb5Conf, String keytab, String principal) {

        System.setProperty("java.security.krb5.conf", krb5Conf);
        System.setProperty("java.security.krb5.keytab", keytab);
        System.setProperty("java.security.krb5.principal", principal);

        this.httpYarnUrl = httpYarnUrl;
        this.authenticatedURL = new AuthenticatedURL(new KerberosAuthenticator());
        token = new AuthenticatedURL.Token();
    }

    public static synchronized Token generateToken(String srvUrl) {
        AuthenticatedURL.Token newToken = new AuthenticatedURL.Token();

        try {
            HttpURLConnection conn = new AuthenticatedURL(
                    new KerberosAuthenticator()).openConnection(
                    //new URL(new URL(srvUrl), "/webhdfs/v1?op=GETHOMEDIRECTORY"),
                    new URL(new URL(srvUrl), "/ws/v1/cluster/apps"),
                    newToken);

            conn.connect();
            conn.disconnect();
        } catch (Exception ex) {
            Log.error(ex.getMessage());
        }

        return newToken;

    }
    
    

    protected static long copy(InputStream input, OutputStream result)
            throws IOException {
        byte[] buffer = new byte[12288]; // 8K=8192 12K=12288 64K=
        long count = 0L;
        int n = 0;
        while (-1 != (n = input.read(buffer))) {
            result.write(buffer, 0, n);
            count += n;
            result.flush();
        }
        result.flush();
        return count;
    }

    /**
     * Report the result in JSON way
     *
     * @param conn
     * @param input
     * @return
     * @throws IOException
     */
    private static String result(HttpURLConnection conn, boolean input) throws IOException {
        StringBuffer sb = new StringBuffer();
        if (input) {
            InputStream is = conn.getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(
                    is, "utf-8"));
            String line = null;

            while ((line = reader.readLine()) != null) {
                sb.append(line);
            }
            reader.close();
            is.close();
        }
        Map<String, Object> result = new HashMap<String, Object>();
        result.put("code", conn.getResponseCode());
        result.put("mesg", conn.getResponseMessage());
        result.put("type", conn.getContentType());
        result.put("data", sb);

        //
        // Convert a Map into JSON string.
        //
        Gson gson = new Gson();
        String json = gson.toJson(result);

        //
        // Convert JSON string back to Map.
        //
        // Type type = new TypeToken<Map<String, Object>>(){}.getType();
        // Map<String, Object> map = gson.fromJson(json, type);
        // for (String key : map.keySet()) {
        // System.out.println("map.get = " + map.get(key));
        // }

        return json;
    }

    public void ensureValidToken() {
        if (!token.isSet()) { // if token is null
            token = generateToken(httpYarnUrl);
        } else {
            long currentTime = new Date().getTime();
            long tokenExpired = Long.parseLong(token.toString().split("&")[3]
                    .split("=")[1]);
            Log.info("[currentTime vs. tokenExpired] " + currentTime + " "
                    + tokenExpired);

            if (currentTime > tokenExpired) { // if the token is expired
                token = generateToken(httpYarnUrl);
            }
        }

    }


	/*
     * ========================================================================
	 * GET
	 * ========================================================================
	 */

    /**
     * <b>GETHOMEDIRECTORY</b>
     * <p>
     * curl -i "http://<HOST>:<PORT>/webhdfs/v1?op=GETHOMEDIRECTORY"
     *
     * @return
     * @throws IOException
     * @throws AuthenticationException
     */
    public String getHomeDirectory() throws IOException,
            AuthenticationException {
        ensureValidToken();

        HttpURLConnection conn = authenticatedURL.openConnection(new URL(
                new URL(httpYarnUrl), "/webhdfs/v1?op=GETHOMEDIRECTORY"), token);
        conn.connect();

        String resp = result(conn, true);
        conn.disconnect();
        return resp;
    }
    
    public String getApps() throws IOException,AuthenticationException {
        ensureValidToken();

        HttpURLConnection conn = authenticatedURL.openConnection(new URL(
                new URL(httpYarnUrl), "/ws/v1/cluster/apps/"), token);
        conn.connect();

        String resp = result(conn, true);
        conn.disconnect();
        return resp;
    }
    
    public String getApps(String appId) throws IOException,AuthenticationException {
        ensureValidToken();

        HttpURLConnection conn = authenticatedURL.openConnection(new URL(
                new URL(httpYarnUrl), "/ws/v1/cluster/apps/" + appId), token);
        conn.connect();

        String resp = result(conn, true);
        conn.disconnect();
        return resp;
    }

    /**
     * <b>OPEN</b>
     * <p>
     * curl -i -L "http://<HOST>:<PORT>/webhdfs/v1<PATH>?op=OPEN
     * [&offset=<LONG>][&length=<LONG>][&buffersize=<INT>]"
     *
     * @param path
     * @param os
     * @throws AuthenticationException
     * @throws IOException
     */
    public String open(String path, OutputStream os)
            throws IOException, AuthenticationException {
        ensureValidToken();

        HttpURLConnection conn = authenticatedURL.openConnection(
                new URL(new URL(httpYarnUrl), MessageFormat.format(
                        "/webhdfs/v1{0}?op=OPEN", URLUtil.encodePath(path))),
                token);
        conn.setRequestMethod("GET");
        conn.setRequestProperty("Content-Type", "application/octet-stream");
        conn.connect();
        InputStream is = conn.getInputStream();
        copy(is, os);
        is.close();
        os.close();
        String resp = result(conn, false);
        conn.disconnect();

        return resp;
    }

    /**
     * <b>GETCONTENTSUMMARY</b>
     * <p>
     * curl -i "http://<HOST>:<PORT>/webhdfs/v1<PATH>?op=GETCONTENTSUMMARY"
     *
     * @param path
     * @return
     * @throws IOException
     * @throws AuthenticationException
     */
    public String getContentSummary(String path) throws
            IOException, AuthenticationException {
        ensureValidToken();

        HttpURLConnection conn = authenticatedURL.openConnection(
                new URL(new URL(httpYarnUrl), MessageFormat.format(
                        "/webhdfs/v1{0}?op=GETCONTENTSUMMARY",
                        URLUtil.encodePath(path))), token);
        conn.setRequestMethod("GET");
        // conn.setRequestProperty("Content-Type", "application/octet-stream");
        conn.connect();
        String resp = result(conn, true);
        conn.disconnect();

        return resp;
    }

    /**
     * <b>LISTSTATUS</b>
     * <p>
     * curl -i "http://<HOST>:<PORT>/webhdfs/v1<PATH>?op=LISTSTATUS"
     *
     * @param path
     * @return
     * @throws IOException
     * @throws AuthenticationException
     */
    public String listStatus(String path) throws
            IOException, AuthenticationException {
        ensureValidToken();
        System.out.println("Token = " + token.isSet());

        HttpURLConnection conn = authenticatedURL.openConnection(
                new URL(new URL(httpYarnUrl), MessageFormat.format(
                        "/webhdfs/v1{0}?op=LISTSTATUS",
                        URLUtil.encodePath(path))), token);
        conn.setRequestMethod("GET");
        conn.connect();
        String resp = result(conn, true);
        conn.disconnect();

        return resp;
    }

    /**
     * <b>GETFILESTATUS</b>
     * <p>
     * curl -i "http://<HOST>:<PORT>/webhdfs/v1<PATH>?op=GETFILESTATUS"
     *
     * @param path
     * @return
     * @throws IOException
     * @throws AuthenticationException
     */
    public String getFileStatus(String path) throws
            IOException, AuthenticationException {
        ensureValidToken();

        HttpURLConnection conn = authenticatedURL.openConnection(
                new URL(new URL(httpYarnUrl), MessageFormat.format(
                        "/webhdfs/v1{0}?op=GETFILESTATUS",
                        URLUtil.encodePath(path))), token);
        conn.setRequestMethod("GET");
        conn.connect();
        String resp = result(conn, true);
        conn.disconnect();

        return resp;
    }

    /**
     * <b>GETFILECHECKSUM</b>
     * <p>
     * curl -i "http://<HOST>:<PORT>/webhdfs/v1<PATH>?op=GETFILECHECKSUM"
     *
     * @param path
     * @return
     * @throws IOException
     * @throws AuthenticationException
     */
    public String getFileCheckSum(String path) throws
            IOException, AuthenticationException {
        String resp = null;
        ensureValidToken();

        HttpURLConnection conn = authenticatedURL.openConnection(
                new URL(new URL(httpYarnUrl), MessageFormat.format(
                        "/webhdfs/v1{0}?op=GETFILECHECKSUM",
                        URLUtil.encodePath(path))), token);

        conn.setRequestMethod("GET");
        conn.connect();
        resp = result(conn, true);
        conn.disconnect();

        return resp;
    }


    public HttpURLConnection getConnection(String url) throws IOException, AuthenticationException {
        ensureValidToken();
        return authenticatedURL.openConnection(new URL(url), token);
    }
    /*
     * ========================================================================
	 * PUT
	 * ========================================================================
	 */

    /**
     * <b>CREATE</b>
     * <p>
     * curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1<PATH>?op=CREATE
     * [&overwrite=<true|false>][&blocksize=<LONG>][&replication=<SHORT>]
     * [&permission=<OCTAL>][&buffersize=<INT>]"
     *
     * @param path
     * @param is
     * @return
     * @throws IOException
     * @throws AuthenticationException
     */
    public String create(String path, InputStream is)
            throws IOException, AuthenticationException {
        String resp = null;
        ensureValidToken();

        String redirectUrl = null;
        HttpURLConnection conn = authenticatedURL
                .openConnection(
                        new URL(new URL(httpYarnUrl), MessageFormat.format(
                                "/webhdfs/v1{0}?op=CREATE",
                                URLUtil.encodePath(path))), token);
        conn.setRequestMethod("PUT");
        conn.setInstanceFollowRedirects(false);
        conn.connect();
        Log.info("Location:" + conn.getHeaderField("Location"));
        System.out.println("Location:" + conn.getHeaderField("Location"));
        resp = result(conn, true);
        if (conn.getResponseCode() == 307) {
            redirectUrl = conn.getHeaderField("Location");
            Log.info("redirectUrl: " + redirectUrl);
        }
        conn.disconnect();

        if (redirectUrl != null) {
            conn = authenticatedURL.openConnection(new URL(redirectUrl), token);
            conn.setRequestMethod("PUT");
            conn.setDoOutput(true);
            conn.setDoInput(true);
            conn.setUseCaches(false);
            conn.setRequestProperty("Content-Type", "application/octet-stream");
            // conn.setRequestProperty("Transfer-Encoding", "chunked");
            final int _SIZE = is.available();
            conn.setRequestProperty("Content-Length", "" + _SIZE);
            conn.setFixedLengthStreamingMode(_SIZE);
            conn.connect();
            OutputStream os = conn.getOutputStream();
            copy(is, os);
            // Util.copyStream(is, os);
            is.close();
            os.close();
            resp = result(conn, false);
            conn.disconnect();
        }

        return resp;
    }

	

	/*
     * ========================================================================
	 * PUT
	 * ========================================================================
	 */

    /**
     * <b>CREATE</b>
     * <p>
     * curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1<PATH>?op=CREATE
     * [&overwrite=<true|false>][&blocksize=<LONG>][&replication=<SHORT>]
     * [&permission=<OCTAL>][&buffersize=<INT>]"
     *
     * @param path
     * @param is
     * @return
     * @throws IOException
     * @throws AuthenticationException
     */
    public String createPOC(String path, InputStream is, HashMap<String, String> map)
            throws IOException, AuthenticationException {
        String resp = null;
        ensureValidToken();

        String redirectUrl = null;
        HttpURLConnection conn = authenticatedURL
                .openConnection(
                        new URL(new URL(httpYarnUrl), MessageFormat.format(
                                "/webhdfs/v1{0}?op=CREATE",
                                URLUtil.encodePath(path))), token);
        conn.setRequestMethod("PUT");
        conn.setInstanceFollowRedirects(false);
        conn.connect();
        Log.info("Location:" + conn.getHeaderField("Location"));
        System.out.println("Location:" + conn.getHeaderField("Location"));
        resp = result(conn, true);
        if (conn.getResponseCode() == 307) {
            String str = conn.getHeaderField("Location");
            if (str.startsWith("http://big1.big")) {
                redirectUrl = str.replaceAll("big1.big", map.get("big1.big"));
            } else if (str.startsWith("http://big2.big")) {
                redirectUrl = str.replaceAll("big2.big", map.get("big2.big"));
            } else if (str.startsWith("http://big3.big")) {
                redirectUrl = str.replaceAll("big3.big", map.get("big3.big"));
            } else {
                redirectUrl = conn.getHeaderField("Location");
            }
        }
        conn.disconnect();

        if (redirectUrl != null) {
            conn = authenticatedURL.openConnection(new URL(redirectUrl), token);
            conn.setRequestMethod("PUT");
            conn.setDoOutput(true);
            conn.setDoInput(true);
            conn.setUseCaches(false);
            conn.setRequestProperty("Content-Type", "application/octet-stream");
            // conn.setRequestProperty("Transfer-Encoding", "chunked");
            final int _SIZE = is.available();
            conn.setRequestProperty("Content-Length", "" + _SIZE);
            conn.setFixedLengthStreamingMode(_SIZE);
            conn.connect();
            OutputStream os = conn.getOutputStream();
            copy(is, os);
            // Util.copyStream(is, os);
            is.close();
            os.close();
            resp = result(conn, false);
            conn.disconnect();
        }

        return resp;
    }

    /**
     * <b>MKDIRS</b>
     * <p>
     * curl -i -X PUT
     * "http://<HOST>:<PORT>/<PATH>?op=MKDIRS[&permission=<OCTAL>]"
     *
     * @param path
     * @return
     * @throws AuthenticationException
     * @throws IOException
     */
    public String mkdirs(String path) throws
            IOException, AuthenticationException {
        String resp = null;
        ensureValidToken();

        HttpURLConnection conn = authenticatedURL
                .openConnection(
                        new URL(new URL(httpYarnUrl), MessageFormat.format(
                                "/webhdfs/v1{0}?op=MKDIRS",
                                URLUtil.encodePath(path))), token);
        conn.setRequestMethod("PUT");
        conn.connect();
        resp = result(conn, true);
        conn.disconnect();

        return resp;
    }

    /**
     * @param srcPath
     * @param destPath
     * @return
     * @throws IOException
     * @throws AuthenticationException
     */
    public String createSymLink(String srcPath, String destPath)
            throws IOException, AuthenticationException {
        String resp = null;
        ensureValidToken();

        HttpURLConnection conn = authenticatedURL.openConnection(
                new URL(new URL(httpYarnUrl), MessageFormat.format(
                        "/webhdfs/v1{0}?op=CREATESYMLINK&destination={1}",
                        URLUtil.encodePath(srcPath),
                        URLUtil.encodePath(destPath))), token);
        conn.setRequestMethod("PUT");
        conn.connect();
        resp = result(conn, true);
        conn.disconnect();

        return resp;
    }

    /**
     * @param srcPath
     * @param destPath
     * @return
     * @throws IOException
     * @throws AuthenticationException
     */
    public String rename(String srcPath, String destPath)
            throws IOException, AuthenticationException {
        String resp = null;
        ensureValidToken();

        HttpURLConnection conn = authenticatedURL.openConnection(
                new URL(new URL(httpYarnUrl), MessageFormat.format(
                        "/webhdfs/v1{0}?op=RENAME&destination={1}",
                        URLUtil.encodePath(srcPath),
                        URLUtil.encodePath(destPath))), token);
        conn.setRequestMethod("PUT");
        conn.connect();
        resp = result(conn, true);
        conn.disconnect();

        return resp;
    }

    /**
     * <b>SETPERMISSION</b>
     * <p>
     * curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1<PATH>?op=SETPERMISSION
     * [&permission=<OCTAL>]"
     *
     * @param path
     * @return
     * @throws AuthenticationException
     * @throws IOException
     */
    public String setPermission(String path) throws
            IOException, AuthenticationException {
        String resp = null;
        ensureValidToken();

        HttpURLConnection conn = authenticatedURL.openConnection(
                new URL(new URL(httpYarnUrl), MessageFormat.format(
                        "/webhdfs/v1{0}?op=SETPERMISSION",
                        URLUtil.encodePath(path))), token);
        conn.setRequestMethod("PUT");
        conn.connect();
        resp = result(conn, true);
        conn.disconnect();

        return resp;
    }

    /**
     * <b>SETOWNER</b>
     * <p>
     * curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1<PATH>?op=SETOWNER
     * [&owner=<USER>][&group=<GROUP>]"
     *
     * @param path
     * @return
     * @throws AuthenticationException
     * @throws IOException
     */
    public String setOwner(String path) throws
            IOException, AuthenticationException {
        String resp = null;
        ensureValidToken();

        HttpURLConnection conn = authenticatedURL.openConnection(
                new URL(new URL(httpYarnUrl),
                        MessageFormat.format("/webhdfs/v1{0}?op=SETOWNER",
                                URLUtil.encodePath(path))), token);
        conn.setRequestMethod("PUT");
        conn.connect();
        resp = result(conn, true);
        conn.disconnect();

        return resp;
    }

    /**
     * <b>SETREPLICATION</b>
     * <p>
     * curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1<PATH>?op=SETREPLICATION
     * [&replication=<SHORT>]"
     *
     * @param path
     * @return
     * @throws AuthenticationException
     * @throws IOException
     */
    public String setReplication(String path) throws
            IOException, AuthenticationException {
        String resp = null;
        ensureValidToken();

        HttpURLConnection conn = authenticatedURL.openConnection(
                new URL(new URL(httpYarnUrl), MessageFormat.format(
                        "/webhdfs/v1{0}?op=SETREPLICATION",
                        URLUtil.encodePath(path))), token);
        conn.setRequestMethod("PUT");
        conn.connect();
        resp = result(conn, true);
        conn.disconnect();

        return resp;
    }

    /**
     * <b>SETTIMES</b>
     * <p>
     * curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1<PATH>?op=SETTIMES
     * [&modificationtime=<TIME>][&accesstime=<TIME>]"
     *
     * @param path
     * @return
     * @throws AuthenticationException
     * @throws IOException
     */
    public String setTimes(String path) throws
            IOException, AuthenticationException {
        String resp = null;
        ensureValidToken();

        HttpURLConnection conn = authenticatedURL.openConnection(
                new URL(new URL(httpYarnUrl),
                        MessageFormat.format("/webhdfs/v1{0}?op=SETTIMES",
                                URLUtil.encodePath(path))), token);
        conn.setRequestMethod("PUT");
        conn.connect();
        resp = result(conn, true);
        conn.disconnect();

        return resp;
    }

	/*
     * ========================================================================
	 * POST
	 * ========================================================================
	 */

    /**
     * curl -i -X POST
     * "http://<HOST>:<PORT>/webhdfs/v1<PATH>?op=APPEND[&buffersize=<INT>]"
     *
     * @param path
     * @param is
     * @return
     * @throws IOException
     * @throws AuthenticationException
     */
    public String append(String path, InputStream is)
            throws IOException, AuthenticationException {
        String resp = null;
        ensureValidToken();

        String redirectUrl = null;
        HttpURLConnection conn = authenticatedURL.openConnection(
                new URL(new URL(httpYarnUrl), MessageFormat.format(
                        "/webhdfs/v1{0}?op=APPEND", path)), token);
        conn.setRequestMethod("POST");
        conn.setInstanceFollowRedirects(false);
        conn.connect();
        Log.info("Location:" + conn.getHeaderField("Location"));
        resp = result(conn, true);
        if (conn.getResponseCode() == 307)
            redirectUrl = conn.getHeaderField("Location");
        conn.disconnect();

        if (redirectUrl != null) {
            conn = authenticatedURL.openConnection(new URL(redirectUrl), token);
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);
            conn.setDoInput(true);
            conn.setUseCaches(false);
            conn.setRequestProperty("Content-Type", "application/octet-stream");
            // conn.setRequestProperty("Transfer-Encoding", "chunked");
            final int _SIZE = is.available();
            conn.setRequestProperty("Content-Length", "" + _SIZE);
            conn.setFixedLengthStreamingMode(_SIZE);
            conn.connect();
            OutputStream os = conn.getOutputStream();
            copy(is, os);
            // Util.copyStream(is, os);
            is.close();
            os.close();
            resp = result(conn, true);
            conn.disconnect();
        }

        return resp;
    }

	/*
     * ========================================================================
	 * DELETE
	 * ========================================================================
	 */

    /**
     * <b>DELETE</b>
     * <p>
     * curl -i -X DELETE "http://<host>:<port>/webhdfs/v1<path>?op=DELETE
     * [&recursive=<true|false>]"
     *
     * @param path
     * @return
     * @throws AuthenticationException
     * @throws IOException
     */
    public String delete(String path) throws
            IOException, AuthenticationException {
        String resp = null;
        ensureValidToken();

        HttpURLConnection conn = authenticatedURL
                .openConnection(
                        new URL(new URL(httpYarnUrl), MessageFormat.format(
                                "/webhdfs/v1{0}?op=DELETE",
                                URLUtil.encodePath(path))), token);
        conn.setRequestMethod("DELETE");
        conn.setInstanceFollowRedirects(false);
        conn.connect();
        resp = result(conn, true);
        conn.disconnect();

        return resp;
    }

    // Begin Getter & Setter
    public String getHttpfsUrl() {
        return httpYarnUrl;
    }

    public void setHttpfsUrl(String httpfsUrl) {
        this.httpYarnUrl = httpfsUrl;
    }

    // End Getter & Setter
}

