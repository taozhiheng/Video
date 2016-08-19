package com.persist.bolts.analysis;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.persist.bean.analysis.PictureResult;
import com.persist.util.helper.FileLogger;
import com.persist.util.tool.analysis.IPictureRecorder;
import java.util.Map;

/**
 * Created by taozhiheng on 16-7-13.
 *
 * record results to hbase
 *
 */
public class PictureRecorderBolt  extends BaseRichBolt {

    private final static String TAG = "PictureRecorderBolt";

    private IPictureRecorder mRecorder;
    private OutputCollector mCollector;

    private FileLogger mLogger;

    private int id;
    private long count = 0;

    public PictureRecorderBolt(IPictureRecorder recorder) {
        this.mRecorder = recorder;
    }

    /**
     * init recorder, actually init HBaseHelper
     * */
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.mCollector = outputCollector;
        mRecorder.prepare();
        id = topologyContext.getThisTaskId();
        mLogger = new FileLogger("picture-record@"+id);
        mLogger.log(TAG+"@"+id, "prepare");
        mRecorder.setLogger(mLogger);
    }

    @Override
    public void cleanup() {
        mRecorder.stop();
        mLogger.close();
    }

    /**
     * record result to habse using HBaseHelper
     * */
    public void execute(Tuple tuple) {
        PictureResult result = (PictureResult) tuple.getValue(0);
        count++;
        String tag = null;
        if(result.description != null)
            tag = result.description.url;
        mLogger.log(TAG+"@"+id, "prepare record: "+tag+","
                +(tag == null? null : result.description.video_id)+","
                +result.percent
                +", total="+count);
        boolean status = mRecorder.recordResult(result);
        mLogger.log(TAG+"@"+id, "record:"+tag+","
                +(tag == null? null : result.description.video_id)+","
                +result.percent
                +", status="+status);
        mCollector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
