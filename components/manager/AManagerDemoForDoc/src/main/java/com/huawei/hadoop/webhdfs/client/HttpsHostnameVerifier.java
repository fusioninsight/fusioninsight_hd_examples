package com.huawei.hadoop.webhdfs.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLSession;

public class HttpsHostnameVerifier implements HostnameVerifier {
    protected static final Logger Log = LoggerFactory.getLogger(HttpsHostnameVerifier.class);

    public boolean verify(String hostname, SSLSession session) {
        return true;
    }
}
