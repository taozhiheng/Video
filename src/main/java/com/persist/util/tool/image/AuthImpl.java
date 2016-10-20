package com.persist.util.tool.image;

import com.persist.util.helper.EncodeHelper;
import com.persist.util.helper.HBaseHelper;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by taozhiheng on 16-10-13.
 * check username and password to authorise user
 */
public class AuthImpl implements IAuth {

    private final static int DEFAULT_CACHE_SIZE = 100;

    private int mCacheSize = DEFAULT_CACHE_SIZE;
    //The cache is used for storing previous pairs of(username, password) to avoid load the same data,
    //and it uses LRU strategy.
    private Map<String, String> mCache;

    //hbase config
    private HBaseHelper mHelper;
    private String quorum;
    private int port;
    private String master;
    private String auth;
    private String table;
    private String family;
    private String column;

    public AuthImpl(String quorum, int port, String master, String auth, String table, String family, String column)
    {
        this(quorum, port, master, auth, table, family, column, DEFAULT_CACHE_SIZE);
    }

    public AuthImpl(String quorum, int port, String master, String auth, String table, String family, String column, int cacheSize)
    {
        if(quorum == null || master == null)
            throw new RuntimeException("HBase quorum or master must not be null");
        this.quorum = quorum;
        this.port = port;
        this.master = master;
        this.auth = auth;
        this.table = table;
        this.family = family;
        this.column = column;

        mCacheSize = cacheSize;
        mCache = new LinkedHashMap<String, String>((int) Math.ceil(mCacheSize / 0.75f) + 1, 0.75f, true)
        {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
                return size() > mCacheSize;
            }
        };
    }

    private void initHBase()
    {
        if(mHelper != null)
            return;
        mHelper = new HBaseHelper(quorum, port, master, auth);
    }

    public void prepare() {
        initHBase();
    }

    public boolean auth(String username, String password) {
        if(username == null || password == null)
            return false;
        //search cache
        String target = mCache.get(username);
        if(target != null)
        {
            return target.equals(password);
        }
        else
        {
            //load password
            try {
                Result result = mHelper.getRow(table, username);
                if(result == null)
                    return false;
                byte[] data = result.getValue(Bytes.toBytes(family), Bytes.toBytes(column));
                String raw = Bytes.toString(data);
                //encode raw password;
                target = EncodeHelper.encode(raw);
                if(target == null)
                    return false;
                //put password to cache
                mCache.put(username, target);
                return target.equals(password);
            }catch (Exception e)
            {
                //read hbase exception
                return false;
            }
        }
    }

    public void stop() {
        if(mHelper != null)
        {
            mHelper.close();
            mHelper = null;
        }
    }
}
