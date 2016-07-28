package com.persist.bolts.analysis;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.persist.bean.analysis.PictureResult;
import com.persist.util.helper.Logger;
import com.persist.util.tool.analysis.IPictureNotifier;
import java.util.Map;

/**
 * Created by zhiheng on 2016/7/5.
 * callback the result from PictureResultBolt,
 * publish msg to redis,
 * and distribute them to PictureRecorderBolt,
 */
public class PictureNotifierBolt extends BaseRichBolt{

    private final static String TAG = "PictureNotifierBolt";

    private OutputCollector mCollector;
    private IPictureNotifier mNotifier;

    public PictureNotifierBolt(IPictureNotifier notifier)
    {
        this.mNotifier = notifier;
    }


    /**
     * hold collector
     * and init notifier, actually init redis connection
     * */
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        Logger.log(TAG, "prepare PictureNotifierBolt");
        this.mCollector = outputCollector;
        mNotifier.prepare();
    }

    @Override
    public void cleanup() {
        super.cleanup();
        mNotifier.stop();
    }

    /**
     * notify redis to update, actually publish result to specific redis channels
     * and emit result to PictureRecorderBolt
     * */
    public void execute(Tuple tuple) {
        PictureResult result = (PictureResult) tuple.getValue(0);
        mNotifier.notifyResult(result);
        mCollector.emit(new Values(result));
        mCollector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("notifier"));
    }
}
