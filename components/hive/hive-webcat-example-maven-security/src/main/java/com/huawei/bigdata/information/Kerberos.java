package com.huawei.bigdata.information;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import java.security.Principal;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class Kerberos {
    public static Logger logger = LoggerFactory.getLogger(Kerberos.class);
    private String principal;
    private String keyTabLocation;

    public Kerberos() {

    }

    public Kerberos(String principal, String keyTabLocation) {

        this.principal = principal;
        this.keyTabLocation = keyTabLocation;
    }

    public Kerberos(String principal, String keyTabLocation, boolean isDebug) {
        this(principal, keyTabLocation);
        if (isDebug) {
            System.setProperty("sun.security.spnego.debug", "true");
            System.setProperty("sun.security.krb5.debug", "true");
        }
    }

    public Kerberos(String principal, String keyTabLocation, String krb5Location, boolean isDebug) {
        this(principal, keyTabLocation, isDebug);
        System.setProperty("java.security.krb5.conf", krb5Location);
    }

    public LoginContext login() {
        LoginContext lc = null;
        Configuration config = new Configuration() {
            @SuppressWarnings("serial")
            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
                return new AppConfigurationEntry[]{new AppConfigurationEntry("com.sun.security.auth.module.Krb5LoginModule",
                        AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, new HashMap<String, Object>() {
                    {
                        put("useTicketCache", "false");
                        put("useKeyTab", "true");
                        put("keyTab", keyTabLocation);
                        //Krb5 in GSS API needs to be refreshed so it does not throw the error
                        //Specified version of key is not available
                        put("refreshKrb5Config", "true");
                        put("principal", principal);
                        put("storeKey", "true");
                        put("doNotPrompt", "true");
                        put("isInitiator", "true");
                        put("debug", "true");
                    }
                })};
            }
        };
        Set<Principal> princ = new HashSet<Principal>(1);
        princ.add(new KerberosPrincipal(principal));
        Subject sub = new Subject(false, princ, new HashSet<Object>(), new HashSet<Object>());
        try {
            //认证模块：Krb5Login
            lc= new LoginContext("Krb5Login", sub, null, config);
            lc.login();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return lc;
    }
}
