package com.persist.bolts.grab;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.persist.bean.grab.VideoInfo;
import com.persist.util.helper.Logger;
import com.persist.util.tool.grab.IVideoNotifier;
import com.persist.util.tool.grab.VideoNotifierImpl;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.Map;

/**
 * Created by taozhiheng on 16-7-21.
 *
 */
public class ResolveBolt extends BaseRichBolt {

    private final static String TAG = "ResolveBolt";
    private Gson mGson;
    private OutputCollector mCollector;
    private IVideoNotifier mNotifier;



    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        mGson = new Gson();
        mCollector = outputCollector;
        mNotifier = new VideoNotifierImpl(
                "develop.finalshares.com", 6379,
                "redis.2016@develop.finalshares.com", new String[]{"rtmp://120.26.103.237:1935/myapp/test1"});
        mNotifier.prepare();
        try {
            Logger.setOutput(new FileOutputStream(TAG, true));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            Logger.setDebug(false);
        }
    }

    public void execute(Tuple tuple) {
        String data = (String) tuple.getValue(0);
        mNotifier.notify("Receive source:"+data);
        Logger.log(TAG, "execute: "+data);
        String url = null;
        VideoInfo videoInfo = new VideoInfo();
        try
        {
            videoInfo = mGson.fromJson(data, VideoInfo.class);
            url = videoInfo.url;
            mCollector.emit(new Values(url, videoInfo));
        }catch (JsonSyntaxException e)
        {
            Logger.log(TAG, "JsonSyntaxException:" + data);
            e.printStackTrace();
        }
        finally {
            mCollector.ack(tuple);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("url", "info"));
    }
}
