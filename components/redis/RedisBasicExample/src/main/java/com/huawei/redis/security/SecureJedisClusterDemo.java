package com.huawei.redis.security;

import java.util.HashSet;
import java.util.Set;

import com.huawei.redis.Const;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

/**
 * 安全模式的JedisCluster使用方式一: auth.conf文件配置方式
 * 安全模式认证过程较慢，建议使用 带超时时间=15000的构造方法
 *
 * 1. 请先登录Manager管理界面创建Redis的角色以及用户，并下载该用户的客户端认证文件。
 * 2. 将下载的krb5.conf、user.keytab文件放到classpath的config路径下。
 * 3. classpath路径下创建config/auth.conf文件，配置文件内容参见本demo：
 *    userName、keyTabFile、krbConfPath根据实际情况修改。其中keyTabFile、krbConfPath是相对classpath的路径。
 */
public class SecureJedisClusterDemo {

    public static void main(String[] args) {
        Set<HostAndPort> hosts = new HashSet<HostAndPort>();
        hosts.add(new HostAndPort(Const.IP_1, Const.PORT_1));
        hosts.add(new HostAndPort(Const.IP_2, Const.PORT_2));
        // add host...
        JedisCluster client = new JedisCluster(hosts, 15000);
        System.out.println(client.set("SecureJedisClusterDemo", "value"));
        System.out.println(client.get("SecureJedisClusterDemo"));
        client.del("SecureJedisClusterDemo");
        client.close();
    }

}
