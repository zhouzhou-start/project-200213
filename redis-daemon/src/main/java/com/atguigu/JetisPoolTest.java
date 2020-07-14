package com.atguigu;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class JetisPoolTest {
    public static void main(String[] args) {
        JedisPool hadoop105 = new JedisPool("hadoop105", 6379);
        Jedis resource = hadoop105.getResource();
        System.out.println(resource.ping());
        resource.close();
    }
}
