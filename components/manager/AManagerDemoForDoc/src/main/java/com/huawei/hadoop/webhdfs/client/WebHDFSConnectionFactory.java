package com.huawei.hadoop.webhdfs.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wesley
 */
public class WebHDFSConnectionFactory {

    protected final Logger Log = LoggerFactory.getLogger(getClass());

    public static enum AuthenticationType {
        KERBEROS, PSEUDO
    }

    private String httpfsUrl = null;
    private String username = null;
    private String password = null;
    private String krb5Conf = null;
    private String keytab = null;
    private String principal = null;

    private AuthenticationType authenticationType = null;
    private WebHDFSConnection webHDFSConnection = null;

    public WebHDFSConnectionFactory(String httpfsUrl, String username, String password) {
        this.httpfsUrl = httpfsUrl;
        this.username = username;
        this.password = password;
        this.authenticationType = AuthenticationType.PSEUDO;
    }

    public WebHDFSConnectionFactory(String httpfsUrl, String krb5Conf, String keytab, String principal) {
        this.httpfsUrl = httpfsUrl;
        this.krb5Conf = krb5Conf;
        this.keytab = keytab;
        this.principal = principal;
        this.authenticationType = AuthenticationType.KERBEROS;
    }

    public WebHDFSConnection getConnection() {
        if (webHDFSConnection == null) {
            if (authenticationType.equals(AuthenticationType.KERBEROS)) {
                webHDFSConnection = new KerberosWebHDFSConnection(httpfsUrl, krb5Conf, keytab, principal);
            } else {
                webHDFSConnection = new PseudoWebHDFSConnection(httpfsUrl, username, password);
            }
        }
        return webHDFSConnection;
    }

    public String getHttpfsUrl() {
        return httpfsUrl;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getKrb5Conf() {
        return krb5Conf;
    }


    public String getKeytab() {
        return keytab;
    }

    public String getPrincipal() {
        return principal;
    }

    public AuthenticationType getAuthenticationType() {
        return authenticationType;
    }

}
