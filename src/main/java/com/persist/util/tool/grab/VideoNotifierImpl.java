package com.persist.util.tool.grab;

import com.persist.util.helper.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Created by taozhiheng on 16-7-20.
 */
public class VideoNotifierImpl implements IVideoNotifier {

    private final static String TAG = "PictureNotifierImpl";

    private Jedis mJedis;
    private String host;
    private int port;
    private String password;
    private String[] channels;

    public VideoNotifierImpl(String host, int port, String password, String[] channels)
    {
        if(host == null || password == null)
            throw  new RuntimeException("Redis host or password must not be null");
        this.host = host;
        this.port = port;
        this.password = password;
        this.channels = channels;
    }

    private void initJedis()
    {
        if(mJedis != null)
            return;
        JedisPool pool = null;
        try {
            // JedisPool依赖于apache-commons-pools1
            JedisPoolConfig config = new JedisPoolConfig();
            pool = new JedisPool(config, host, port, 3000, password);
            mJedis = pool.getResource();
            Logger.log(TAG, pool.toString());
        } catch (Exception e) {
            e.printStackTrace();
            Logger.log(TAG, "redis exception");

        }
    }

    public void prepare() {
        initJedis();
    }

    public Jedis getJedis()
    {
        return mJedis;
    }

    public void notify(String msg) {
        if(mJedis == null) {
            initJedis();
        }

        if(mJedis != null && channels != null)
        {
            for (String channel : channels)
            {
                mJedis.publish(channel, msg);
            }
        }
        Logger.log(TAG, "notify:" + msg);
    }

    public void stop() {
        if(mJedis != null)
            mJedis.close();
    }
}
