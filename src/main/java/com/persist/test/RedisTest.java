package com.persist.test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Created by taozhiheng on 16-8-11.
 *
 */
public class RedisTest {

    public static void main(String[] args) throws Exception
    {
        String host = "develop.finalshares.com";
        int port = 6379;
        String password = "redis.2016@develop.finalshares.com";

        String channel = args[0];

        Jedis jedis = null;
        JedisPool pool;
        try {
            // JedisPool依赖于apache-commons-pools1
            JedisPoolConfig config = new JedisPoolConfig();
            pool = new JedisPool(config, host, port, 6000, password);
            jedis = pool.getResource();
        } catch (Exception e) {
            e.printStackTrace();
        }

        for(int i = 1; i < args.length; i++)
        {
            if(jedis != null) {
                jedis.publish(channel, "1-"+args[i]);
            }
        }
        if(jedis != null) {
            jedis.close();

        }
    }
}
