package com.persist.bolts.analysis;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.persist.bean.analysis.PictureResult;
import com.persist.util.helper.FileLogger;
import com.persist.util.tool.analysis.IPictureNotifier;
import java.util.Map;

/**
 * Created by zhiheng on 2016/7/5.
 *
 * publish results to redis,
 * and trigger PictureRecorderBolt to record
 *
 */
public class PictureNotifierBolt extends BaseRichBolt{

    private final static String TAG = "PictureNotifierBolt";

    private OutputCollector mCollector;
    private IPictureNotifier mNotifier;

    private FileLogger mLogger;
    private int id;
    private long count = 0;

    public PictureNotifierBolt(IPictureNotifier notifier)
    {
        this.mNotifier = notifier;
    }


    /**
     * hold collector
     * and init notifier, actually init redis connection
     * */
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.mCollector = outputCollector;
        mNotifier.prepare();
        id = topologyContext.getThisTaskId();
        mLogger = new FileLogger("picture-notify@"+id);
        mLogger.log(TAG+"@"+id, "prepare");
    }

    @Override
    public void cleanup() {
        super.cleanup();
        mNotifier.stop();
        mLogger.close();
    }

    /**
     * notify redis to update, actually publish result to specific redis channels
     * and emit result to PictureRecorderBolt
     * */
    public void execute(Tuple tuple) {
        PictureResult result = (PictureResult) tuple.getValue(0);
        count++;
        String tag = null;
        if(result.description != null)
            tag = result.description.url;
        mLogger.log(TAG+"@"+id, "prepare notify:"+tag+","
                +(tag == null? null : result.description.video_id)+","
                +result.percent
                +", total="+count);
        boolean status = mNotifier.notifyResult(result);
        mCollector.emit(new Values(result));

        mLogger.log(TAG+"@"+id, "notify:"+tag+","
                +(tag == null? null : result.description.video_id)+","
                +result.percent
                +" status="+status);
        mCollector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("notifier"));
    }
}
