package com.huawei.redis.security;

import java.util.HashSet;
import java.util.Set;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import com.huawei.jredis.client.GlobalConfig;
import com.huawei.jredis.client.auth.AuthConfiguration;
import com.huawei.redis.Const;

/**
 * 安全认证使用方式二： API设置方式
 * 
 * 1. 构造AuthConfiguration对象，指定keytab文件路径，principal，krb5.conf文件路径
 *    若krb5.conf文件路径不指定，默认读取java.security.krb5.conf系统参数的值
 * 
 * 2. GlobalConfig.setAuthConfiguration设置为上面创建的AuthConfiguration对象
 * 
 * 3. 创建JedisCluster对象，创建方式同非安全一样
 */
public class SecureJedisClusterDemo2 {
    public static void main(String[] args) {
        System.setProperty("java.security.krb5.conf", "krb5.conf file path");
        AuthConfiguration authConfiguration = new AuthConfiguration("keytab file path", "principal");
        GlobalConfig.setAuthConfiguration(authConfiguration);
        
        Set<HostAndPort> hosts = new HashSet<HostAndPort>();
        hosts.add(new HostAndPort(Const.IP_1, Const.PORT_1));
        JedisCluster client = new JedisCluster(hosts, 5000);
        
        client.set("test-key", System.currentTimeMillis() + "");
        System.out.println(client.get("test-key"));
        client.del("test-key");
        client.close();
    }
}
