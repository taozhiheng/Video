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
import com.persist.util.helper.FileLogger;
import java.util.Map;

/**
 * Created by taozhiheng on 16-7-21.
 *
 * resolve video info from json string,
 * and trigger GrabBolt to control grab processes
 *
 */
public class ResolveBolt extends BaseRichBolt {

    private final static String TAG = "ResolveBolt";
    private Gson mGson;
    private OutputCollector mCollector;

    private FileLogger mLogger;
    private int id;
    private long count = 0;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        mGson = new Gson();
        mCollector = outputCollector;
        id = topologyContext.getThisTaskId();
        mLogger = new FileLogger("resolve@"+id);
        mLogger.log(TAG+"@"+id, "prepare");
    }

    public void execute(Tuple tuple) {
        String data = (String) tuple.getValue(0);
        count++;
        mLogger.log(TAG+"@"+id, "resolve data: "+data);
        String url;
        VideoInfo videoInfo;
        try
        {
            //resolve json string
            videoInfo = mGson.fromJson(data, VideoInfo.class);
            if(videoInfo != null)
            {
                url = videoInfo.url;
                mCollector.emit(new Values(url, videoInfo));
            }
        }
        catch (JsonSyntaxException e)
        {
            e.printStackTrace(mLogger.getPrintWriter());
            mLogger.getPrintWriter().flush();
            e.printStackTrace();
        }
        finally
        {
            mLogger.log(TAG+"@"+id, "resolve msg total="+count);
            mCollector.ack(tuple);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("url", "info"));
    }
}
