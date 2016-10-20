package com.persist.bolts.image;

import backtype.storm.drpc.DRPCInvocationsClient;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import com.google.gson.Gson;
import com.persist.bean.image.ImageInfo;
import com.persist.util.helper.FileLogger;
import com.persist.util.tool.image.IRecorder;
import org.apache.thrift7.TException;
import org.json.simple.JSONValue;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by taozhiheng on 16-8-4.
 *
 * implement return bolt to replace ReturnResults
 *
 * return results to client
 *
 */
public class ReturnBolt extends BaseRichBolt {

    private final static String TAG = "ReturnBolt";

    private final static int DEFAULT_RECENT_COUNT = 100;

    //record  host:port -> client instance
    private Map<String, DRPCInvocationsClient> mClients;
    private OutputCollector mCollector;

    private FileLogger mLogger;
    private int id;
    private long count = 0;

    private Gson mGson;

    private IRecorder mRecorder;
    private int mRecentCount = 100;

    public ReturnBolt()
    {
        this(DEFAULT_RECENT_COUNT);
    }

    public ReturnBolt(int recentCount)
    {
        this(null, recentCount);
    }

    public ReturnBolt(IRecorder recorder)
    {
        this(recorder, DEFAULT_RECENT_COUNT);
    }

    public ReturnBolt(IRecorder recorder, int recentCount)
    {
        this.mRecorder = recorder;
        this.mRecentCount = recentCount;
    }

    @Override
    public void cleanup() {
        super.cleanup();
        mLogger.close();
        if(mRecorder != null)
        {
            mRecorder.stop();
        }
    }

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.mCollector = collector;
        mClients = new HashMap<String, DRPCInvocationsClient>();

        id = context.getThisTaskId();
        mLogger = new FileLogger("return@"+id);
        mLogger.log(TAG+"@"+id, "prepare");

        mGson = new Gson();

        if(mRecorder != null)
        {
            mRecorder.setLogger(mLogger);
            mRecorder.prepare();
        }
    }

    public void execute(Tuple input) {
//        String result = (String) input.getValue(0);
        ImageInfo info = (ImageInfo) input.getValue(0);
        String result = mGson.toJson(info);
        String returnInfo = (String) input.getValue(1);
        String user = (String) input.getValue(2);

        if(returnInfo!=null)
        {
            Map retMap = (Map) JSONValue.parse(returnInfo);
            final String host = (String) retMap.get("host");
            final int port = Utils.getInt(retMap.get("port"));
            String id = (String) retMap.get("id");
            String key = host+":"+port;
            DRPCInvocationsClient client;
            //get client from buffer
            if(mClients.containsKey(key))
            {
                client = mClients.get(key);
            }
            //create new client
            else
            {
                client = new DRPCInvocationsClient(host, port);
                mClients.put(key, client);
            }
            try {
                //return response message to client
                client.result(id, result);
                mLogger.log(TAG+"@"+this.id, "Succeed to return results: "+result+", user: "+user);
                //record this service to hbase ...
                if(mRecorder != null && info != null && info.urls != null && info.values != null)
                {
                    StringBuilder urls = new StringBuilder();
                    StringBuilder values = new StringBuilder();
                    if(info.urls.length > 0)
                    {
                        int len = info.urls.length;
                        if(len > mRecentCount)
                            len = mRecentCount;
                        urls.append(info.urls[0]);
                        for(int i = 1; i < len; i++)
                        {
                            urls.append(',').append(info.urls[i]);
                        }
                    }
                    if(info.values.length > 0)
                    {
                        int len = info.values.length;
                        if(len > mRecentCount)
                            len = mRecentCount;
                        values.append(info.values[0]);
                        for(int i = 1; i < len; i++)
                        {
                            values.append(',').append(info.values[i]);
                        }
                    }
                    boolean status = mRecorder.record(user, info.urls.length,
                            urls.toString(), values.toString());
                    mLogger.log(TAG+"@"+this.id, "record: user="+user+", size="+info.urls.length+", status="+status);
                }
                count++;
                mLogger.log(TAG+"@"+this.id, "total="+count);
                mCollector.ack(input);
            } catch(TException e) {
                e.printStackTrace(mLogger.getPrintWriter());
                mLogger.getPrintWriter().flush();
                mCollector.fail(input);
            }
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
