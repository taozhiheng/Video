package com.persist.bolts.image;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.persist.bean.image.ImageInfo;
import com.persist.util.helper.FileLogger;
import com.persist.util.tool.image.IAuth;

import java.util.Map;

/**
 * Created by taozhiheng on 16-8-3.
 *
 * resolve urls from json string,
 * download images from urls,
 * and trigger PictureBolt to predict
 *
 */
public class UrlBolt extends BaseRichBolt {

    private final static String TAG = "ImageBolt";

    private OutputCollector mCollector;
    private Gson mGson;


    private FileLogger mLogger;
    private int id;
    private long count = 0;

    //the tool for authentication.If it is null, the bolt will not check authentication.
    private IAuth mAuth;

    public UrlBolt()
    {
    }

    public UrlBolt(IAuth auth)
    {
        this.mAuth = auth;
    }

    @Override
    public void cleanup() {
        super.cleanup();
        mLogger.close();
        if(mAuth != null)
        {
            mAuth.stop();
        }
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.mCollector = outputCollector;
        mGson = new Gson();
        id = topologyContext.getThisTaskId();
        mLogger = new FileLogger("url@"+id);
        mLogger.log(TAG+"@"+id, "prepare, the IAuth instance is: "+mAuth);
        if(mAuth != null)
        {
            mAuth.prepare();
        }
    }

    public void execute(Tuple tuple) {
        String data = tuple.getString(0);
        String returnInfo = tuple.getValue(1).toString();
        mLogger.log(TAG+"@"+id, "receive data:"+data);
        count++;
        try
        {
            //resolve json string
            ImageInfo info = mGson.fromJson(data, ImageInfo.class);

            //authentication
            if(mAuth != null)
            {
                if(!mAuth.auth(info.username, info.password))
                {
                    mLogger.log(TAG+"@"+id, "invalid authentication: username="+info.username+", password="+info.password);
                    mCollector.ack(tuple);
                    return;
                }
                else
                {
                    mLogger.log(TAG+"@"+id, "valid authentication: username="+info.username);
                }
            }

            if(info.urls != null && info.urls.length > 0)
            {
                String key = System.currentTimeMillis()+"@"+id;
                //emit each url so that the download tasks can be executed in multi threads
                for(String url : info.urls)
                {
                    mCollector.emit(new Values(url, key, info.urls.length, returnInfo, info.user));
                }
                mLogger.log(TAG+"@"+id, "url size="+info.urls.length+", message total="+count);
            }
        }
        catch (JsonSyntaxException e)
        {
            e.printStackTrace(mLogger.getPrintWriter());
            mLogger.getPrintWriter().flush();
        }
        finally
        {
            mCollector.ack(tuple);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("url", "key", "size", "result-info", "user"));
    }
}
