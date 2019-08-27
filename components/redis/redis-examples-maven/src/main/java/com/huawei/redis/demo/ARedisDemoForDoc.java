package com.huawei.redis.demo;

import com.huawei.medis.BatchException;
import com.huawei.medis.ClusterBatch;
import com.huawei.redis.Const;
import com.huawei.redis.LoginUtil;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.*;

public class ARedisDemoForDoc {
    static {
        //日志配置文件
        PropertyConfigurator.configure(ARedisDemoForDoc.class.getClassLoader().getResource("conf/log4j.properties").getPath());
    }
    private static final Logger LOGGER = Logger.getLogger(ARedisDemoForDoc.class);
    private static JedisCluster client;
    private static ClusterBatch pipeline;

    public static void main(String[] args) {

        try {
            //安全认证
            System.setProperty("redis.authentication.jaas", "true");
            if (System.getProperty("redis.authentication.jaas", "false").equals("true")) {
                //把principal修改成自己的用户名
                String principal = "my_fwc";
                LoginUtil.setJaasFile(principal, getResource("conf/user.keytab"));
                LoginUtil.setKrb5Config(getResource("conf/krb5.conf"));
            }
        } catch (IOException e) {
            LOGGER.error("Failed to init security configuration", e);
            return;
        }

        //通过指定集群中一个或多个实例的IP跟端口号，创建JedisCluster实例。
        Set<HostAndPort> hosts = new HashSet<HostAndPort>();
        hosts.add(new HostAndPort(Const.IP_1, Const.PORT_1));
        hosts.add(new HostAndPort(Const.IP_2, Const.PORT_2));
        // add more host...

        // 连接、请求超时时长，时间单位ms
        int timeout = 5000;
        //JedisCluster封装了java访问redis集群的各种操作，包括初始化连接、请求重定向等。
        client = new JedisCluster(hosts, timeout);

        testString();
        testList();
        testHash();
        testSet();
        testSortedSet();
        testPipeline();
        testKey();

        destory();
    }

    //使用完后要关闭资源
    public static void destory() {
        if (pipeline != null) {
            pipeline.close();
        }
        if (client != null) {
            client.close();
        }
    }



    public static void testString() {
        LOGGER.info("----------start testString-------");
        String key = "sid-user01";

        // 保存用户的会话ID，并设置过期时间
        //Setex 命令为指定的 key 设置值及其过期时间。如果 key 已经存在， SETEX 命令将会替换旧的值。
        client.setex(key, 5, "A0BC9869FBC92933255A37A1D21167B2");
        String sessionId = client.get(key);
        LOGGER.info("User " + key + ", session id: " + sessionId);
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            LOGGER.warn("InterruptedException");
        }
        sessionId = client.get(key);
        LOGGER.info("User " + key + ", session id: " + sessionId);

        key = "message";

        //设置指定 key 的值
        client.set(key, "hello");
        String value = client.get(key);
        LOGGER.info("Value: " + value);
        // key 已经存在并且是一个字符串， APPEND 命令将指定的 value 追加到该 key 原来值（value）的末尾。
        client.append(key, " world");
        value = client.get(key);
        LOGGER.info("After append, value: " + value);

        client.del(key);
    }

    public static void testList() {
        LOGGER.info("----------start testList-------");
        String key = "messages";

        //Right push在列表尾部（右边）添加一个或多个值
        client.rpush(key, "Hello how are you?");
        client.rpush(key, "Fine thanks. I'm having fun with redis.");
        client.rpush(key, "I should look into this NOSQL thing ASAP");

        //lrange 获取列表指定范围内的元素
        List<String> messages = client.lrange(key, 0, -1);
        LOGGER.info("All messages: " + messages);

        //llen 获取列表长度
        long len = client.llen(key);
        LOGGER.info("Message count: " + len);

        //lpop 移出并获取列表的第一个元素
        String message = client.lpop(key);
        LOGGER.info("First message: " + message);
        len = client.llen(key);
        LOGGER.info("After one pop, message count: " + len);

        client.del(key);
    }

    public static void testHash() {
        LOGGER.info("----------start testHash-------");
        String key = "userinfo-001";

        //将哈希表 key 中的字段 field 的值设为 value ,和 Map.put()类似。
        client.hset(key, "id", "J001");
        client.hset(key, "name", "John");
        client.hset(key, "gender", "male");
        client.hset(key, "age", "35");
        client.hset(key, "salary", "1000000");

        // 和 Map.get()类似
        String id = client.hget(key, "id");
        String name = client.hget(key, "name");
        LOGGER.info("User " + id + "'s name is " + name);

        //hgetAll 获取在哈希表中指定 key 的所有字段和值
        Map<String, String> user = client.hgetAll(key);
        LOGGER.info(user);
        client.del(key);

        key = "userinfo-002";
        Map<String, String> user2 = new HashMap<String, String>();
        user2.put("id", "L002");
        user2.put("name", "Lucy");
        user2.put("gender", "female");
        user2.put("age", "25");
        user2.put("salary", "200000");
        //hmset 同时将多个 field-value (域-值)对设置到哈希表 key 中。
        client.hmset(key, user2);
        //hincrBy 为哈希表 key 中的指定字段的整数值加上增量 increment 。
        client.hincrBy(key, "salary", 50000);
        id = client.hget(key, "id");
        String salary = client.hget(key, "salary");
        LOGGER.info("User " + id + "'s salary is " + salary);

        //获取所有哈希表中的字段,类似 Map.keySet()
        Set<String> keys = client.hkeys(key);
        LOGGER.info("all fields: " + keys);
        //获取哈希表中所有值，类似 Map.values()
        List<String> values = client.hvals(key);
        LOGGER.info("all values: " + values);

        //获取所有给定字段的值
        values = client.hmget(key, "id", "name");
        LOGGER.info("partial field values: " + values);

        // 查看哈希表 key 中，指定的字段是否存在，类似 Map.containsKey();
        boolean exist = client.hexists(key, "gender");
        LOGGER.info("Exist field gender? " + exist);

        // 删除一个或多个哈希表字段,Map.remove();
        client.hdel(key, "age");
        keys = client.hkeys(key);
        LOGGER.info("after del field age, rest fields: " + keys);

        client.del(key);
    }

    public static void testSet() {
        LOGGER.info("----------start testSet-------");
        String key = "sets";
        //向集合添加一个或多个成员
        client.sadd(key, "HashSet");
        client.sadd(key, "SortedSet");
        client.sadd(key, "TreeSet");

        // 获取集合的成员数,类似 Set.size()
        long size = client.scard(key);
        LOGGER.info("Set size: " + size);

        client.sadd(key, "SortedSet");
        size = client.scard(key);
        LOGGER.info("Set size: " + size);

        //smembers 返回集合中的所有成员
        Set<String> sets = client.smembers(key);
        LOGGER.info("Set: " + sets);

        //srem 移除集合中一个或多个成员
        client.srem(key, "SortedSet");
        sets = client.smembers(key);
        LOGGER.info("Set: " + sets);

        //sismember 判断 member 元素是否是集合 key 的成员
        boolean ismember = client.sismember(key, "TreeSet");
        LOGGER.info("TreeSet is set's member: " + ismember);
        client.del(key);
    }

    public static void testSortedSet() {
        LOGGER.info("----------start testSortedSet-------");
        String key = "hackers";

        //向有序集合添加一个或多个成员，或者更新已存在成员的分数
        client.zadd(key, 1940, "Alan Kay");
        client.zadd(key, 1953, "Richard Stallman");
        client.zadd(key, 1965, "Yukihiro Matsumoto");
        client.zadd(key, 1916, "Claude Shannon");
        client.zadd(key, 1969, "Linus Torvalds");
        client.zadd(key, 1912, "Alan Turing");

        // 通过索引区间返回有序集合 指定区间内的成员
        Set<String> setValues = client.zrange(key, 0, -1);
        LOGGER.info("All hackers: " + setValues);

        //获取有序集合的成员数
        long size = client.zcard(key);
        LOGGER.info("Size: " + size);

        //返回有序集中，成员的分数值
        Double score = client.zscore(key, "Linus Torvalds");
        LOGGER.info("Score: " + score);

        //计算在有序集合中指定区间分数的成员数
        long count = client.zcount(key, 1960, 1969);
        LOGGER.info("Count: " + count);

        // 返回有序集中指定区间内的成员，通过索引，分数从高到底
        Set<String> setValues2 = client.zrevrange(key, 0, -1);
        LOGGER.info("All hackers 2: " + setValues2);

        //移除有序集合中的一个或多个成员
        client.zrem(key, "Linus Torvalds");
        setValues = client.zrange(key, 0, -1);
        LOGGER.info("All hackers: " + setValues);

        client.del(key);
    }

    //pipeline通过减少客户端与redis的通信次数来实现降低往返延时时间，提高了 redis 服务的性能。
    public static void testPipeline() {
        LOGGER.info("----------start testPipeline-------");

        try {
            if (pipeline == null) {
                pipeline = client.getPipeline();
            }

            pipeline.hset("website", "google", "www.google.cn");
            pipeline.hset("website", "baidu", "www.baidu.com");
            pipeline.hset("website", "sina", "www.sina.com");

            Map<String, String> map = new HashMap<String, String>();
            map.put("cardid", "123456");
            map.put("username", "jzkangta");
            pipeline.hmset("hash", map);
            // 提交，对于不需要返回值的命令，直接调用JedisCluster 的sync()方法即可。
            pipeline.sync();

            pipeline.hget("website", "google");
            pipeline.hget("website", "baidu");
            pipeline.hget("website", "sina");

            // syncAndReturnAll则会返回所有命令的结果，返回类型为：List<Object>。
            List<Object> result = pipeline.syncAndReturnAll();
            LOGGER.info("Result: " + result);

            client.del("website");
            client.del("hash");
        } catch (Exception e) {
            LOGGER.error("Exception", e);
        }
    }

    public static void testKey() {
        LOGGER.info("----------start testKey-------");
        String key = "test-key";

        client.set(key, "test");
        //为给定 key 设置过期时间，以秒计。
        client.expire(key, 5);
        //以秒为单位，返回给定 key 的剩余生存时间(TTL, time to live)。
        long ttl = client.ttl(key);
        LOGGER.info("TTL: " + ttl);

        //返回 key 所储存的值的类型。
        String type = client.type(key);
        // 类型可以是 string, list, hash, set, zset
        LOGGER.info("KEY type: " + type);
        // key 存在时删除 key。
        client.del(key);

        //Right push在列表尾部（右边）添加一个或多个值
        client.rpush(key, "1");
        client.rpush(key, "4");
        client.rpush(key, "6");
        client.rpush(key, "3");
        client.rpush(key, "8");
        List<String> result = client.lrange(key, 0, -1);
        LOGGER.info("List: " + result);

        result = client.sort(key);
        LOGGER.info("Sort list: " + result);

        // key 存在时删除 key。
        client.del(key);
    }

    private static String getResource(String name) {
        ClassLoader cl = ARedisDemoForDoc.class.getClassLoader();
        if (cl == null) {
            return null;
        }
        URL url = cl.getResource(name);
        if (url == null) {
            return null;
        }

        try {
            return URLDecoder.decode(url.getPath(), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            return null;
        }
    }
}
