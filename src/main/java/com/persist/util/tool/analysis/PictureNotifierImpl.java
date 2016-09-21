package com.persist.util.tool.analysis;

import com.google.gson.Gson;
import com.persist.bean.analysis.PictureResult;
import com.persist.util.helper.FileLogger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisConnectionException;

/**
 * Created by zhiheng on 2016/7/5.
 *
 * hold a Jedis instance
 * before starting working, the method prepare() must be invoked
 *
 * notify redis the new msg
 *
 */
public class PictureNotifierImpl implements IPictureNotifier {

    private final static String TAG = "PictureNotifierImpl";

    private Jedis mJedis;
    private String host;
    private int port;
    private String password;
    private String[] channels;
    private Gson mGson;

    private FileLogger mLogger;

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
            pool = new JedisPool(config, host, port, 6000, password);
            mJedis = pool.getResource();
        } catch (Exception e) {
            if(mLogger != null)
            {
                e.printStackTrace(mLogger.getPrintWriter());
                mLogger.getPrintWriter().flush();
            }
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

    public void setLogger(FileLogger logger) {
        this.mLogger = logger;
    }

    public boolean notifyResult(PictureResult result) {
        boolean ok = false;
        if(mJedis == null) {
            initJedis();
        }
        if(mGson == null)
            initGson();
        if(mJedis != null && result.description != null && result.description.video_id != null)
        {
            String msg = mGson.toJson(result);
            try
            {
                mJedis.publish(result.description.video_id, msg);
                mJedis.publish(result.description.video_id, "ok:"+result.ok+", percent:"+result.percent);
                ok = true;
                if(mLogger != null)
                {
                    mLogger.log(TAG, msg);
                }
            }
            catch (JedisConnectionException e)
            {
                if(mLogger != null) {
                    e.printStackTrace(mLogger.getPrintWriter());
                    mLogger.getPrintWriter().flush();
                }
                stop();
//                initJedis();
            }
        }
        return ok;
    }

    public void stop() {
        if(mJedis != null) {
            mJedis.close();
            mJedis = null;
        }
    }
}
