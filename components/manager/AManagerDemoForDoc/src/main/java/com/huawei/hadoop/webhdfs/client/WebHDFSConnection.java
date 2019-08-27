package com.huawei.hadoop.webhdfs.client;

import org.apache.hadoop.security.authentication.client.AuthenticationException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

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
public interface WebHDFSConnection {

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
     */
    public String getHomeDirectory() throws IOException,
            AuthenticationException;

    /**
     * <b>OPEN</b>
     * <p>
     * curl -i -L "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=OPEN
     * [&offset=<LONG>][&length=<LONG>][&buffersize=<INT>]"
     *
     * @param path
     * @param os
     * @throws IOException
     */
    public String open(String path, OutputStream os)
            throws IOException, AuthenticationException;

    /**
     * <b>GETCONTENTSUMMARY</b>
     * <p>
     * curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=GETCONTENTSUMMARY"
     *
     * @param path
     * @return
     * @throws IOException
     */
    public String getContentSummary(String path) throws
            IOException, AuthenticationException;

    /**
     * <b>LISTSTATUS</b>
     * <p>
     * curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=LISTSTATUS"
     *
     * @param path
     * @return
     * @throws IOException
     */
    public String listStatus(String path) throws
            IOException, AuthenticationException;

    /**
     * <b>GETFILESTATUS</b>
     * <p>
     * curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=GETFILESTATUS"
     *
     * @param path
     * @return
     * @throws IOException
     */
    public String getFileStatus(String path) throws
            IOException, AuthenticationException;

    /**
     * <b>GETFILECHECKSUM</b>
     * <p>
     * curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=GETFILECHECKSUM"
     *
     * @param path
     * @return
     * @throws IOException
     */
    public String getFileCheckSum(String path) throws
            IOException, AuthenticationException;

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
     */
    public String create(String path, InputStream is)
            throws IOException, AuthenticationException;

    /**
     * <b>MKDIRS</b>
     * <p>
     * curl -i -X PUT
     * "http://<HOST>:<PORT>/<PATH>?op=MKDIRS[&permission=<OCTAL>]"
     *
     * @param path
     * @return
     * @throws IOException
     */
    public String mkdirs(String path) throws
            IOException, AuthenticationException;

    /**
     * <b>CREATESYMLINK</b>
     * <p>
     * curl -i -X PUT "http://<HOST>:<PORT>/<PATH>?op=CREATESYMLINK
     * &destination=<PATH>[&createParent=<true|false>]"
     *
     * @param path
     * @return
     * @throws IOException
     */
    public String createSymLink(String srcPath, String destPath)
            throws IOException, AuthenticationException;

    /**
     * <b>RENAME</b>
     * <p>
     * curl -i -X PUT "http://<HOST>:<PORT>/<PATH>?op=RENAME
     * &destination=<PATH>[&createParent=<true|false>]"
     *
     * @param path
     * @return
     * @throws IOException
     */
    public String rename(String srcPath, String destPath)
            throws IOException, AuthenticationException;

    /**
     * <b>SETPERMISSION</b>
     * <p>
     * curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=SETPERMISSION
     * [&permission=<OCTAL>]"
     *
     * @param path
     * @return
     * @throws IOException
     */
    public String setPermission(String path) throws
            IOException, AuthenticationException;

    /**
     * <b>SETOWNER</b>
     * <p>
     * curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=SETOWNER
     * [&owner=<USER>][&group=<GROUP>]"
     *
     * @param path
     * @return
     * @throws IOException
     */
    public String setOwner(String path) throws
            IOException, AuthenticationException;

    /**
     * <b>SETREPLICATION</b>
     * <p>
     * curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=SETREPLICATION
     * [&replication=<SHORT>]"
     *
     * @param path
     * @return
     * @throws IOException
     */
    public String setReplication(String path) throws
            IOException, AuthenticationException;

    /**
     * <b>SETTIMES</b>
     * <p>
     * curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=SETTIMES
     * [&modificationtime=<TIME>][&accesstime=<TIME>]"
     *
     * @param path
     * @return
     * @throws IOException
     */
    public String setTimes(String path) throws
            IOException, AuthenticationException;

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
     */
    public String append(String path, InputStream is)
            throws IOException, AuthenticationException;

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
     * @throws IOException
     */
    public String delete(String path) throws
            IOException, AuthenticationException;

}
