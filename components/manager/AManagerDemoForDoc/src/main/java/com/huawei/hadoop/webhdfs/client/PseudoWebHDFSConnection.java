package com.huawei.hadoop.webhdfs.client;

import com.google.gson.Gson;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL.Token;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.client.Authenticator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.MessageFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * ===== HTTP GET <br/>
 * <li>OPEN (see FileSystem.open) <li>GETFILESTATUS (see
 * FileSystem.getFileStatus) <li>LISTSTATUS (see FileSystem.listStatus) <li>
 * GETCONTENTSUMMARY (see FileSystem.getContentSummary) <li>GETFILECHECKSUM (see
 * FileSystem.getFileChecksum) <li>GETHOMEDIRECTORY (see
 * FileSystem.getHomeDirectory) <li>GETDELEGATIONTOKEN (see
 * FileSystem.getDelegationToken) <li>GETDELEGATIONTOKENS (see
 * FileSystem.getDelegationTokens) <br/>
 * ===== HTTP PUT <br/> <li>CREATE (see FileSystem.create) <li>MKDIRS (see
 * FileSystem.mkdirs) <li>CREATESYMLINK (see FileContext.createSymlink) <li>
 * RENAME (see FileSystem.rename) <li>SETREPLICATION (see
 * FileSystem.setReplication) <li>SETOWNER (see FileSystem.setOwner) <li>
 * SETPERMISSION (see FileSystem.setPermission) <li>SETTIMES (see
 * FileSystem.setTimes) <li>RENEWDELEGATIONTOKEN (see
 * FileSystem.renewDelegationToken) <li>CANCELDELEGATIONTOKEN (see
 * FileSystem.cancelDelegationToken) <br/>
 * ===== HTTP POST <br/>
 * APPEND (see FileSystem.append) <br/>
 * ===== HTTP DELETE <br/>
 * DELETE (see FileSystem.delete)
 */
public class PseudoWebHDFSConnection implements WebHDFSConnection {

    protected static final Logger Log = LoggerFactory
            .getLogger(PseudoWebHDFSConnection.class);

    private String httpfsUrl = null;
    private String principal = null;
    private String password = null;

    private Token token = new AuthenticatedURL.Token();
    private AuthenticatedURL authenticatedURL = new AuthenticatedURL(
            new PseudoAuthenticator(principal));

    public PseudoWebHDFSConnection() {
    }

    public PseudoWebHDFSConnection(String httpfsUrl, String principal,
                                   String password) {
        this.httpfsUrl = httpfsUrl;
        this.principal = principal;
        this.password = password;
        this.authenticatedURL = new AuthenticatedURL(new PseudoAuthenticator(
                principal));
    }

    public static synchronized Token generateToken(String srvUrl, String princ,
                                                   String passwd) {
        AuthenticatedURL.Token newToken = new AuthenticatedURL.Token();
        Authenticator authenticator = new PseudoAuthenticator(princ);
        try {

            String spec = MessageFormat.format(
                    "/webhdfs/v1/?op=GETHOMEDIRECTORY&user.name={0}", princ);
            HttpURLConnection conn = new AuthenticatedURL(authenticator)
                    .openConnection(new URL(new URL(srvUrl), spec), newToken);

            conn.connect();

            conn.disconnect();

        } catch (Exception ex) {
            Log.error(ex.getMessage());
            Log.error("[" + princ + ":" + passwd + "]@" + srvUrl, ex);
            // WARN
            // throws  IOException,
            // AuthenticationException, InterruptedException
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
    private static String result(HttpURLConnection conn, boolean input)
            throws IOException {
        StringBuffer sb = new StringBuffer();
        if (input) {
            InputStream is = conn.getInputStream();
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(is));
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
        Log.info("json = " + json);

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
            token = generateToken(httpfsUrl, principal, password);
        } else {
            long currentTime = new Date().getTime();
            long tokenExpired = Long.parseLong(token.toString().split("&")[3]
                    .split("=")[1]);
            Log.debug("[currentTime vs. tokenExpired] " + currentTime + " "
                    + tokenExpired);

            if (currentTime > tokenExpired) { // if the token is expired
                token = generateToken(httpfsUrl, principal, password);
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
     * curl -i "http://<HOST>:<PORT>/webhdfs/v1/?op=GETHOMEDIRECTORY"
     *
     * @return
     * @throws IOException
     * @throws AuthenticationException
     */
    public String getHomeDirectory() throws IOException,
            AuthenticationException {
        ensureValidToken();
        String spec = MessageFormat.format(
                "/webhdfs/v1/?op=GETHOMEDIRECTORY&user.name={0}",
                this.principal);
        HttpURLConnection conn = authenticatedURL.openConnection(new URL(
                new URL(httpfsUrl), spec), token);
        conn.connect();
        String resp = result(conn, true);
        conn.disconnect();
        return resp;
    }

    /**
     * <b>OPEN</b>
     * <p>
     * curl -i -L "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=OPEN
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
        String spec = MessageFormat.format(
                "/webhdfs/v1{0}?op=OPEN&user.name={1}",
                URLUtil.encodePath(path), this.principal);
        HttpURLConnection conn = authenticatedURL.openConnection(new URL(
                new URL(httpfsUrl), spec), token);
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
     * curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=GETCONTENTSUMMARY"
     *
     * @param path
     * @return
     * @throws IOException
     * @throws AuthenticationException
     */
    public String getContentSummary(String path) throws
            IOException, AuthenticationException {
        ensureValidToken();
        String spec = MessageFormat.format(
                "/webhdfs/v1{0}?op=GETCONTENTSUMMARY&user.name={1}",
                URLUtil.encodePath(path), this.principal);
        HttpURLConnection conn = authenticatedURL.openConnection(new URL(
                new URL(httpfsUrl), spec), token);
        conn.setRequestMethod("GET");
        conn.connect();
        String resp = result(conn, true);
        conn.disconnect();

        return resp;
    }

    /**
     * <b>LISTSTATUS</b>
     * <p>
     * curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=LISTSTATUS"
     *
     * @param path
     * @return
     * @throws IOException
     * @throws AuthenticationException
     */
    public String listStatus(String path) throws
            IOException, AuthenticationException {
        ensureValidToken();
        String spec = MessageFormat.format(
                "/webhdfs/v1{0}?op=LISTSTATUS&user.name={1}",
                URLUtil.encodePath(path), this.principal);
        HttpURLConnection conn = authenticatedURL.openConnection(new URL(
                new URL(httpfsUrl), spec), token);
        conn.setRequestMethod("GET");
        conn.connect();
        String resp = result(conn, true);
        conn.disconnect();

        return resp;
    }

    /**
     * <b>GETFILESTATUS</b>
     * <p>
     * curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=GETFILESTATUS"
     *
     * @param path
     * @return
     * @throws IOException
     * @throws AuthenticationException
     */
    public String getFileStatus(String path) throws
            IOException, AuthenticationException {
        ensureValidToken();
        String spec = MessageFormat.format(
                "/webhdfs/v1{0}?op=GETFILESTATUS&user.name={1}",
                URLUtil.encodePath(path), this.principal);
        HttpURLConnection conn = authenticatedURL.openConnection(new URL(
                new URL(httpfsUrl), spec), token);
        conn.setRequestMethod("GET");
        conn.connect();
        String resp = result(conn, true);
        conn.disconnect();

        return resp;
    }

    /**
     * <b>GETFILECHECKSUM</b>
     * <p>
     * curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=GETFILECHECKSUM"
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
        String spec = MessageFormat.format(
                "/webhdfs/v1{0}?op=GETFILECHECKSUM&user.name={1}",
                URLUtil.encodePath(path), this.principal);
        HttpURLConnection conn = authenticatedURL.openConnection(new URL(
                new URL(httpfsUrl), spec), token);

        conn.setRequestMethod("GET");
        conn.connect();
        resp = result(conn, true);
        conn.disconnect();

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
     * curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=CREATE
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
        String spec = MessageFormat.format(
                "/webhdfs/v1{0}?op=CREATE&user.name={1}",
                URLUtil.encodePath(path), this.principal);
        String redirectUrl = null;

        HttpURLConnection conn = authenticatedURL.openConnection(new URL(
                new URL(httpfsUrl), spec), token);
        conn.setRequestMethod("PUT");
        conn.setInstanceFollowRedirects(false);
        conn.connect();
        Log.info("Location:" + conn.getHeaderField("Location"));
        resp = result(conn, true);
        if (conn.getResponseCode() == 307)
            redirectUrl = conn.getHeaderField("Location");
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
        String spec = MessageFormat.format(
                "/webhdfs/v1{0}?op=MKDIRS&user.name={1}",
                URLUtil.encodePath(path), this.principal);
        HttpURLConnection conn = authenticatedURL.openConnection(new URL(
                new URL(httpfsUrl), spec), token);
        conn.setRequestMethod("PUT");
        conn.connect();
        resp = result(conn, true);
        conn.disconnect();

        return resp;
    }

    /**
     * <b>CREATESYMLINK</b>
     * <p>
     * curl -i -X PUT "http://<HOST>:<PORT>/<PATH>?op=CREATESYMLINK
     * &destination=<PATH>[&createParent=<true|false>]"
     *
     * @param path
     * @return
     * @throws AuthenticationException
     * @throws IOException
     */
    public String createSymLink(String srcPath, String destPath)
            throws IOException, AuthenticationException {
        String resp = null;
        ensureValidToken();
        String spec = MessageFormat
                .format("/webhdfs/v1{0}?op=CREATESYMLINK&destination={1}&user.name={2}",
                        URLUtil.encodePath(srcPath),
                        URLUtil.encodePath(destPath), this.principal);
        HttpURLConnection conn = authenticatedURL.openConnection(new URL(
                new URL(httpfsUrl), spec), token);
        conn.setRequestMethod("PUT");
        conn.connect();
        resp = result(conn, true);
        conn.disconnect();

        return resp;
    }

    /**
     * <b>RENAME</b>
     * <p>
     * curl -i -X PUT "http://<HOST>:<PORT>/<PATH>?op=RENAME
     * &destination=<PATH>[&createParent=<true|false>]"
     *
     * @param path
     * @return
     * @throws AuthenticationException
     * @throws IOException
     */
    public String rename(String srcPath, String destPath)
            throws IOException, AuthenticationException {
        String resp = null;
        ensureValidToken();
        String spec = MessageFormat.format(
                "/webhdfs/v1{0}?op=RENAME&destination={1}&user.name={2}",
                URLUtil.encodePath(srcPath), URLUtil.encodePath(destPath),
                this.principal);
        HttpURLConnection conn = authenticatedURL.openConnection(new URL(
                new URL(httpfsUrl), spec), token);
        conn.setRequestMethod("PUT");
        conn.connect();
        resp = result(conn, true);
        conn.disconnect();

        return resp;
    }

    /**
     * <b>SETPERMISSION</b>
     * <p>
     * curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=SETPERMISSION
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
        String spec = MessageFormat.format(
                "/webhdfs/v1{0}?op=SETPERMISSION&user.name={1}",
                URLUtil.encodePath(path), this.principal);
        HttpURLConnection conn = authenticatedURL.openConnection(new URL(
                new URL(httpfsUrl), spec), token);
        conn.setRequestMethod("PUT");
        conn.connect();
        resp = result(conn, true);
        conn.disconnect();

        return resp;
    }

    /**
     * <b>SETOWNER</b>
     * <p>
     * curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=SETOWNER
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
        String spec = MessageFormat.format(
                "/webhdfs/v1{0}?op=SETOWNER&user.name={1}",
                URLUtil.encodePath(path), this.principal);
        HttpURLConnection conn = authenticatedURL.openConnection(new URL(
                new URL(httpfsUrl), spec), token);
        conn.setRequestMethod("PUT");
        conn.connect();
        resp = result(conn, true);
        conn.disconnect();

        return resp;
    }

    /**
     * <b>SETREPLICATION</b>
     * <p>
     * curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=SETREPLICATION
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
        String spec = MessageFormat.format(
                "/webhdfs/v1{0}?op=SETREPLICATION&user.name={1}",
                URLUtil.encodePath(path), this.principal);
        HttpURLConnection conn = authenticatedURL.openConnection(new URL(
                new URL(httpfsUrl), spec), token);
        conn.setRequestMethod("PUT");
        conn.connect();
        resp = result(conn, true);
        conn.disconnect();

        return resp;
    }

    /**
     * <b>SETTIMES</b>
     * <p>
     * curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=SETTIMES
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
        String spec = MessageFormat.format(
                "/webhdfs/v1{0}?op=SETTIMES&user.name={1}",
                URLUtil.encodePath(path), this.principal);
        HttpURLConnection conn = authenticatedURL.openConnection(new URL(
                new URL(httpfsUrl), spec), token);
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
     * "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=APPEND[&buffersize=<INT>]"
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
        String spec = MessageFormat.format(
                "/webhdfs/v1{0}?op=APPEND&user.name={1}",
                URLUtil.encodePath(path), this.principal);
        String redirectUrl = null;
        HttpURLConnection conn = authenticatedURL.openConnection(new URL(
                new URL(httpfsUrl), spec), token);
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
     * curl -i -X DELETE "http://<host>:<port>/webhdfs/v1/<path>?op=DELETE
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
        String spec = MessageFormat.format(
                "/webhdfs/v1{0}?op=DELETE&user.name={1}",
                URLUtil.encodePath(path), this.principal);
        HttpURLConnection conn = authenticatedURL.openConnection(new URL(
                new URL(httpfsUrl), spec), token);
        conn.setRequestMethod("DELETE");
        conn.setInstanceFollowRedirects(false);
        conn.connect();
        resp = result(conn, true);
        conn.disconnect();

        return resp;
    }

    // Begin Getter & Setter
    public String getHttpfsUrl() {
        return httpfsUrl;
    }

    public void setHttpfsUrl(String httpfsUrl) {
        this.httpfsUrl = httpfsUrl;
    }

    public String getPrincipal() {
        return principal;
    }

    public void setPrincipal(String principal) {
        this.principal = principal;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    // End Getter & Setter
}