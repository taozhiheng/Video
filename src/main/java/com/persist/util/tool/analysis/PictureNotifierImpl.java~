package com.persist.util.tool.analysis;

import com.google.gson.Gson;
import com.persist.bean.analysis.PictureResult;
import com.persist.util.helper.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Created by zhiheng on 2016/7/5.
 * hold a Jedis instance
 * before starting working, the method prepare() must be invoked
 *
 * notify redis the new msg
 */
public class PictureNotifierImpl implements IPictureNotifier {

    private final static String TAG = "PictureNotifierImpl";

    private Jedis mJedis;
    private String host;
    private int port;
    private String password;
    private String[] channels;
    private Gson mGson;

    public PictureNotifierImpl(String host, int port, String password, String[] channels)
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
            System.out.println(pool);
            mJedis = pool.getResource();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void initGson()
    {
        mGson = new Gson();
    }

    public void prepare() {
        initJedis();
        initGson();
    }

    public void notifyResult(PictureResult result) {
        if(mJedis == null) {
            initJedis();
        }
        if(mGson == null)
            initGson();
        String msg = mGson.toJson(result);
//        String msg = result.description.url;
	    mJedis.publish(result.description.video_id, msg);
//        if(channels != null) {
//            for (String channel : channels)
//            {
//                mJedis.publish(channel, msg);
//            }
//        }
        Logger.log(TAG, "notify:"+msg);
    }

    public void stop() {
        if(mJedis != null)
            mJedis.close();
    }
}
