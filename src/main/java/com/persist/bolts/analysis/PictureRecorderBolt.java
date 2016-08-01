package com.persist.bolts.analysis;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.persist.bean.analysis.PictureResult;
import com.persist.util.helper.Logger;
import com.persist.util.tool.analysis.IPictureRecorder;
import com.persist.util.tool.grab.IVideoNotifier;
import com.persist.util.tool.grab.VideoNotifierImpl;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.Map;

/**
 * Created by taozhiheng on 16-7-13.
 *
 * record the result from PictureNotifierBolt(actually, PictureResultBolt)
 */
public class PictureRecorderBolt  extends BaseRichBolt {

    private final static String TAG = "PictureNotifierBolt";

    private IPictureRecorder mRecorder;
    private OutputCollector mCollector;

//    private IVideoNotifier mNotifier;

    public PictureRecorderBolt(IPictureRecorder recorder) {
        this.mRecorder = recorder;
    }

    /**
     * init recorder, actually init HBaseHelper
     * */
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        Logger.log(TAG, "prepare PictureNotifierBolt");
        this.mCollector = outputCollector;
        mRecorder.prepare();
//        mNotifier = new VideoNotifierImpl(
//                "develop.finalshares.com", 6379,
//                "redis.2016@develop.finalshares.com", new String[]{"record"});
//        mNotifier.prepare();
        try {
            Logger.setOutput(new FileOutputStream("VideoAnalyzer", true));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            Logger.setDebug(false);
        }
    }

    @Override
    public void cleanup() {
        mRecorder.stop();
//        mNotifier.stop();
    }

    /**
     * record result to habse using HBaseHelper
     * */
    public void execute(Tuple tuple) {
        PictureResult result = (PictureResult) tuple.getValue(0);
        boolean status = mRecorder.recordResult(result);
//        mNotifier.notify("Record image:"+result.description.url+", status:"+status);
        Logger.log(TAG, "record: "+result.description.url+","+result.description.video_id+","+result.percent+" status:"+status);
        mCollector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
