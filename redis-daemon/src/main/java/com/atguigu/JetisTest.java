package com.atguigu;

import redis.clients.jedis.Jedis;

public class JetisTest {
    public static void main(String[] args) {
        Jedis jedis = new Jedis("hadoop105", 6379);
        String ping = jedis.ping();
        System.out.println(ping);
        jedis.close();
    }
}
