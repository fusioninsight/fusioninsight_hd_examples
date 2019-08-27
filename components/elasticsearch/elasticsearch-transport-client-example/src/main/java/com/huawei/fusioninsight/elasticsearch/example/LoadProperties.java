package com.huawei.fusioninsight.elasticsearch.example;

import com.huawei.fusioninsight.elasticsearch.example.util.LoginUtil;
import com.huawei.fusioninsight.elasticsearch.transport.common.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.transport.TransportAddress;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

public class LoadProperties {
    private static final Logger LOG = LogManager.getLogger(LoadProperties.class);
    private static String ipPattern = "((2[0-4]\\d|25[0-5]|[01]?\\d\\d?)\\.){3}(2[0-4]\\d|25[0-5]|[01]?\\d\\d?)";
    private static Properties properties = new Properties();
    public static String keytabPath;
    public static final String CONFIGURATION_FILE_NAME = "esParams.properties";


    public static Configuration loadProperties() throws IOException {
        initProperties();
        Configuration configuration = new Configuration();
        configuration.setClusterName(loadClusterName());
        configuration.setTransportAddress(loadTransportAddress());
        configuration.setSecureMode(loadIsSecureMode());
        if (configuration.isSecureMode()) {
            configuration.setPrincipal(loadPrincipal());
            configuration.setKeyTabPath(loadPath("keytabPath"));
            configuration.setKrb5Path(loadPath("krb5Path"));
            keytabPath = configuration.getKeyTabPath();
            setSecurityConfig(configuration.getPrincipal(), keytabPath, configuration.getKrb5Path());
        }
        configuration.setSniff(loadIsSniff());
        LOG.info("configuration:" + configuration);
        return configuration;
    }

    private static void initProperties() {
        try {
            String configPath = System.getProperty("user.dir") + File.separator + "conf" + File.separator + CONFIGURATION_FILE_NAME;
            properties.load(new FileInputStream(new File(configPath)));
        } catch (Exception e) {
            LOG.error("Failed to load properties file conf/{}.", CONFIGURATION_FILE_NAME);
            throw new IllegalArgumentException();
        }
    }

    public static String loadClusterName() {
        String clusterName = properties.getProperty("clusterName");
        if (null == clusterName || clusterName.isEmpty()) {
            LOG.error("clusterName is empty, please configure it in conf/{}", CONFIGURATION_FILE_NAME);
            throw new IllegalArgumentException();
        }
        return clusterName;
    }

    private static Set<TransportAddress> loadTransportAddress() {
        String serverHosts = properties.getProperty("esServerHosts");
        if (null == serverHosts || serverHosts.isEmpty()) {
            LOG.error("Please configure esServerHosts in conf/{}.", CONFIGURATION_FILE_NAME);
            LOG.error("The format of esServerHosts is ip1:port1,ip2:port2,ipn,portn");
            throw new IllegalArgumentException();
        }
        String[] hosts = serverHosts.split(",");
        Set<TransportAddress> transportAddresses = new HashSet<>(hosts.length);
        for (String host : hosts) {
            String[] ipAndPort = host.split(":");
            String esClientIP = ipAndPort[0];
            String esClientPort = ipAndPort[1];
            if (!Pattern.matches(ipPattern, esClientIP)) {
                LOG.error("The configuration  clientIP format is wrong, please configure it in conf/{}.", CONFIGURATION_FILE_NAME);
                throw new IllegalArgumentException();
            }
            if (null == esClientPort || esClientPort.isEmpty()) {
                LOG.error("The configuration  esClientIPPort is empty, please configure it in conf/{}.", CONFIGURATION_FILE_NAME);
                throw new IllegalArgumentException();
            }
            if (new Integer(esClientPort) % 2 == 0) {
                LOG.warn("The configuration esClientIPPort may be wrong, please check it in conf/esParams.properties",
                    CONFIGURATION_FILE_NAME);
            }
            try {
                transportAddresses.add(new TransportAddress(InetAddress.getByName(esClientIP), Integer.valueOf(esClientPort)));
            } catch (Exception e) {
                LOG.error("Init esServerHosts occur error : {}", e.getMessage());
                throw new IllegalArgumentException();
            }
        }
        return transportAddresses;
    }

    private static String loadPath(String path) {
        String loadedPath = properties.getProperty(path);
        if (null == loadedPath || loadedPath.isEmpty()) {
            loadedPath = System.getProperty("user.dir") + File.separator + "conf" + File.separator;
            LOG.warn(path + " is empty, using the default path.");
        }
        return loadedPath;
    }

    private static boolean loadIsSecureMode() {
        return !properties.getProperty("isSecureMode").equals("false");
    }

    private static boolean loadIsSniff() {
        return !properties.getProperty("isSniff").equals("false");
    }

    private static String loadPrincipal() {
        String principal = properties.getProperty("principal");
        if (null == principal || principal.isEmpty()) {
            LOG.error("Please configure principal in conf/{}.", CONFIGURATION_FILE_NAME);
            throw new IllegalArgumentException();
        }
        return principal;
    }

    private static void setSecurityConfig(String principal, String keytabPath, String krb5Path) throws IOException {
        LoginUtil.setJaasFile(principal, keytabPath + "user.keytab");
        System.setProperty("es.security.indication", "true");
        LoginUtil.setKrb5Config(krb5Path + "krb5.conf");
    }

}
