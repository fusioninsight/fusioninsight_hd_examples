package com.huawei.hadoop.webhdfs.client;


import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.HadoopKerberosName;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.client.Authenticator;
import org.apache.hadoop.security.authentication.client.ConnectionConfigurator;
import org.apache.hadoop.security.authentication.util.AuthToken;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosKey;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URL;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;

public class KerberosAuthenticator implements Authenticator {

    private static Logger Log = LoggerFactory.getLogger(KerberosAuthenticator.class);

    /**
     * HTTP header used by the SPNEGO server endpoint during an authentication sequence.
     */
    public static final String WWW_AUTHENTICATE = "WWW-Authenticate";

    /**
     * HTTP header used by the SPNEGO client endpoint during an authentication sequence.
     */
    public static final String AUTHORIZATION = "Authorization";

    /**
     * HTTP header prefix used by the SPNEGO client/server endpoints during an authentication sequence.
     */
    public static final String NEGOTIATE = "Negotiate";

    private static final String AUTH_HTTP_METHOD = "OPTIONS";

    private static final String HADOOP_LOGIN_MODULE = "hadoop-keytab-kerberos";

    /*
    * Defines the Kerberos configuration that will be used to obtain the Kerberos principal from the
    * Kerberos cache.
    */
    private class HadoopConfiguration extends
            javax.security.auth.login.Configuration {

        private final Map<String, String> HADOOP_KERBEROS_OPTION = new HashMap<String, String>();

        private final AppConfigurationEntry HADOOP_KERBEROS_CONF = new AppConfigurationEntry(
                KerberosUtil.getKrb5LoginModuleName(), LoginModuleControlFlag.REQUIRED,
                HADOOP_KERBEROS_OPTION);

        @Override
        public AppConfigurationEntry[] getAppConfigurationEntry(String name) {

            if (HADOOP_LOGIN_MODULE.equals(name)) {
                HADOOP_KERBEROS_OPTION.put("debug", "false");
                HADOOP_KERBEROS_OPTION.put("doNotPrompt", "true");
                HADOOP_KERBEROS_OPTION.put("useKeyTab", "true");
                HADOOP_KERBEROS_OPTION.put("storeKey", "true");
                HADOOP_KERBEROS_OPTION.put("refreshKrb5Config", "true");

                String keytab = System.getProperty("java.security.krb5.keytab");
                String principal = System.getProperty("java.security.krb5.principal");

                Log.info("keytab:" + keytab);
                Log.info("principal:" + principal);
                HADOOP_KERBEROS_OPTION.put("keyTab", keytab);
                HADOOP_KERBEROS_OPTION.put("principal", principal);

                return new AppConfigurationEntry[]{HADOOP_KERBEROS_CONF};
            }
            return null;
        }
    }

    private URL url;
    private HttpURLConnection conn;
    private Base64 base64;
    private ConnectionConfigurator connConfigurator;

    /**
     * Sets a {@link ConnectionConfigurator} instance to use for
     * configuring connections.
     *
     * @param configurator the {@link ConnectionConfigurator} instance.
     */
    @Override
    public void setConnectionConfigurator(ConnectionConfigurator configurator) {
        connConfigurator = configurator;
    }

    /**
     * Performs SPNEGO authentication against the specified URL.
     * <p>
     * If a token is given it does a NOP and returns the given token.
     * <p>
     * If no token is given, it will perform the SPNEGO authentication sequence using an
     * HTTP <code>OPTIONS</code> request.
     *
     * @param url   the URl to authenticate against.
     * @param token the authentication token being used for the user.
     * @throws IOException             if an IO error occurred.
     * @throws AuthenticationException if an authentication error occurred.
     */
    @Override
    public void authenticate(URL url, AuthenticatedURL.Token token)
            throws IOException, AuthenticationException {
        if (!token.isSet()) {
            this.url = url;
            base64 = new Base64(0);
            conn = (HttpURLConnection) url.openConnection();
            if (connConfigurator != null) {
                conn = connConfigurator.configure(conn);
            }
            conn.setRequestMethod(AUTH_HTTP_METHOD);
            conn.connect();

            boolean needFallback = false;
            if (conn.getResponseCode() == HttpURLConnection.HTTP_OK) {
                Log.debug("JDK performed authentication on our behalf.");
                // If the JDK already did the SPNEGO back-and-forth for
                // us, just pull out the token.
                AuthenticatedURL.extractToken(conn, token);
                if (isTokenKerberos(token)) {
                    return;
                }
                needFallback = true;
            }
            if (!needFallback && isNegotiate()) {
                Log.debug("Performing our own SPNEGO sequence.");
                doSpnegoSequence(token);
            } else {
                Log.debug("Using fallback authenticator sequence.");
                Authenticator auth = getFallBackAuthenticator();
                // Make sure that the fall back authenticator have the same
                // ConnectionConfigurator, since the method might be overridden.
                // Otherwise the fall back authenticator might not have the information
                // to make the connection (e.g., SSL certificates)
                auth.setConnectionConfigurator(connConfigurator);
                auth.authenticate(url, token);
            }
        }
    }

    /**
     * If the specified URL does not support SPNEGO authentication, a fallback {@link Authenticator} will be used.
     * <p>
     * This implementation returns a {@link PseudoAuthenticator}.
     *
     * @return the fallback {@link Authenticator}.
     */
    protected Authenticator getFallBackAuthenticator() {
        Authenticator auth = new PseudoAuthenticator();
        if (connConfigurator != null) {
            auth.setConnectionConfigurator(connConfigurator);
        }
        return auth;
    }

    /*
     * Check if the passed token is of type "kerberos" or "kerberos-dt"
     */
    private boolean isTokenKerberos(AuthenticatedURL.Token token)
            throws AuthenticationException {
        if (token.isSet()) {
            AuthToken aToken = AuthToken.parse(token.toString());
            if (aToken.getType().equals("kerberos") ||
                    aToken.getType().equals("kerberos-dt")) {
                return true;
            }
        }
        return false;
    }

    /*
    * Indicates if the response is starting a SPNEGO negotiation.
    */
    private boolean isNegotiate() throws IOException {
        boolean negotiate = false;
        if (conn.getResponseCode() == HttpURLConnection.HTTP_UNAUTHORIZED) {
            String authHeader = conn.getHeaderField(WWW_AUTHENTICATE);
            negotiate = authHeader != null && authHeader.trim().startsWith(NEGOTIATE);
        }
        return negotiate;
    }

    /**
     * Implements the SPNEGO authentication sequence interaction using the current default principal
     * in the Kerberos cache (normally set via kinit).
     *
     * @param token the authentication token being used for the user.
     * @throws IOException             if an IO error occurred.
     * @throws AuthenticationException if an authentication error occurred.
     */
    private void doSpnegoSequence(AuthenticatedURL.Token token) throws IOException, AuthenticationException {
        try {
            AccessControlContext context = AccessController.getContext();
            Subject subject = Subject.getSubject(context);
            if (subject == null
                    || (subject.getPrivateCredentials(KerberosKey.class).isEmpty()
                    && subject.getPrivateCredentials(KerberosTicket.class).isEmpty())) {

                Configuration conf = new Configuration();
                String HADOOP_SECURITY_AUTHENTICATION = "hadoop.security.authentication";
                conf.set(HADOOP_SECURITY_AUTHENTICATION, "kerberos");
                subject = new Subject();
                LoginContext loginCtx = new LoginContext(HADOOP_LOGIN_MODULE, subject,
                        null, new HadoopConfiguration());
                loginCtx.login();
                HadoopKerberosName.setConfiguration(conf);

            }

            Subject.doAs(subject, new PrivilegedExceptionAction<Void>() {

                @Override
                public Void run() throws Exception {
                    GSSContext gssContext = null;
                    try {
                        GSSManager gssManager = GSSManager.getInstance();
                        String hostname = InetAddress.getByName(
                                KerberosAuthenticator.this.url.getHost()).getHostName();
                        String servicePrincipal = KerberosUtil.getServicePrincipal("HTTP", hostname);
                        Oid oid = KerberosUtil.getOidInstance("NT_GSS_KRB5_PRINCIPAL");
                        GSSName serviceName = gssManager.createName(servicePrincipal,
                                oid);
                        oid = KerberosUtil.getOidInstance("GSS_KRB5_MECH_OID");
                        gssContext = gssManager.createContext(serviceName, oid, null,
                                GSSContext.DEFAULT_LIFETIME);
                        gssContext.requestCredDeleg(true);
                        gssContext.requestMutualAuth(true);

                        byte[] inToken = new byte[0];
                        byte[] outToken;
                        boolean established = false;

                        // Loop while the context is still not established
                        while (!established) {
                            outToken = gssContext.initSecContext(inToken, 0, inToken.length);
                            if (outToken != null) {
                                sendToken(outToken);
                            }

                            if (!gssContext.isEstablished()) {
                                inToken = readToken();
                            } else {
                                established = true;
                            }
                        }
                    } finally {
                        if (gssContext != null) {
                            gssContext.dispose();
                            gssContext = null;
                        }
                    }
                    return null;
                }
            });
        } catch (PrivilegedActionException ex) {
            throw new AuthenticationException(ex.getException());
        } catch (LoginException ex) {
            throw new AuthenticationException(ex);
        }
        AuthenticatedURL.extractToken(conn, token);
    }

    /*
    * Sends the Kerberos token to the server.
    */
    private void sendToken(byte[] outToken) throws IOException {
        String token = base64.encodeToString(outToken);
        conn = (HttpURLConnection) url.openConnection();
        if (connConfigurator != null) {
            conn = connConfigurator.configure(conn);
        }
        conn.setRequestMethod(AUTH_HTTP_METHOD);
        conn.setRequestProperty(AUTHORIZATION, NEGOTIATE + " " + token);
        conn.connect();
    }

    /*
    * Retrieves the Kerberos token returned by the server.
    */
    private byte[] readToken() throws IOException, AuthenticationException {
        int status = conn.getResponseCode();
        if (status == HttpURLConnection.HTTP_OK || status == HttpURLConnection.HTTP_UNAUTHORIZED) {
            String authHeader = conn.getHeaderField(WWW_AUTHENTICATE);
            if (authHeader == null || !authHeader.trim().startsWith(NEGOTIATE)) {
                throw new AuthenticationException("Invalid SPNEGO sequence, '" + WWW_AUTHENTICATE +
                        "' header incorrect: " + authHeader);
            }
            String negotiation = authHeader.trim().substring((NEGOTIATE + " ").length()).trim();
            return base64.decode(negotiation);
        }
        throw new AuthenticationException("Invalid SPNEGO sequence, status code: " + status);
    }
}
