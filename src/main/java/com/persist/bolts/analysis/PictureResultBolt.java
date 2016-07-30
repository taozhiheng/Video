package com.persist.bolts.analysis;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.persist.bean.analysis.PictureKey;
import com.persist.bean.analysis.PictureResult;
import com.persist.util.helper.Logger;
import com.persist.util.tool.analysis.IPictureCalculator;
import com.persist.util.tool.grab.IVideoNotifier;
import com.persist.util.tool.grab.VideoNotifierImpl;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.List;
import java.util.Map;

/**
 * Created by zhiheng on 2016/7/5.
 * calculate images from PictureEntityBolt
 * and distribute them to PictureResultBolt
 */
public class PictureResultBolt extends BaseRichBolt {

    private final static String TAG = "PictureResultBolt";

    private OutputCollector mCollector;
    private IPictureCalculator mCalculator;
    private Gson mGson;


    public PictureResultBolt(IPictureCalculator calculator)
    {
        this.mCalculator = calculator;
    }


    @Override
    public void cleanup() {
        super.cleanup();
    }

    /**
     * init collector
     * and init calculator, actually init redis
     * */
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.mCollector = outputCollector;
        this.mGson = new Gson();
        Logger.log(TAG, "prepare PictureResultBolt");
        mCalculator.prepare();
        try {
            Logger.setOutput(new FileOutputStream("VideoAnalyzer", true));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            Logger.setDebug(false);
        }
    }

    /**
     * resolve the msg from string to json to object
     * and emit result to PictureRecorderBolt
     * */
    public void execute(Tuple tuple) {
        String data = tuple.getString(0);
        Logger.log(TAG, "resolve result:"+data);
        PictureKey pictureKey = new PictureKey();
        try {
            pictureKey = mGson.fromJson(data, PictureKey.class);
        } catch (JsonSyntaxException e) {
            e.printStackTrace();
            Logger.log(TAG, "JsonSyntaxException:"+e.getMessage());
        }
        List<PictureResult> list = mCalculator.calculateImage(pictureKey);
        if(list != null && list.size() > 0)
        {
            for(PictureResult result : list)
            {
                mCollector.emit(new Values(result));
                Logger.log(TAG, "emit result:"+mGson.toJson(result));
            }
        }
        mCollector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("result"));
    }
}
