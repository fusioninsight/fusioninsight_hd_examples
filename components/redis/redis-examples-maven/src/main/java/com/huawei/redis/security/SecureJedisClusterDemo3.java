package com.huawei.redis.security;

import java.util.HashSet;
import java.util.Set;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import com.huawei.redis.Const;

/**
 * 安全认证使用方式三： 使用jaas文件配置方式
 * 
 * 必须设置redis.authentication.jaas, java.security.auth.login.config, java.security.krb5.conf 3个系统参数
 * 可通过java命令行-D参数设置
 * 
 * jaas.conf文件样例:
Client{
com.sun.security.auth.module.Krb5LoginModule required
useKeyTab=true
keyTab="/xxx/user.keytab"
principal="xxxx"
useTicketCache=false
storeKey=true
debug=false;
};

 */
public class SecureJedisClusterDemo3 {

    public static void main(String[] args) {
        System.setProperty("redis.authentication.jaas", "true");
        System.setProperty("java.security.auth.login.config", "jaas.conf file path");
        System.setProperty("java.security.krb5.conf", "krb5.conf file path");
        // jaas.conf文件Section名字，不设置的话默认值为Client, 区分大小写
        // System.setProperty("redis.sasl.clientconfig", "redisClient");

        Set<HostAndPort> hosts = new HashSet<HostAndPort>();
        hosts.add(new HostAndPort(Const.IP_1, Const.PORT_1));
        JedisCluster client = new JedisCluster(hosts, 5000);

        client.set("test-key", System.currentTimeMillis() + "");
        System.out.println(client.get("test-key"));
        client.del("test-key");
        client.close();
    }

}
