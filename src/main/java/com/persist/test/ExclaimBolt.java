package com.persist.test;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Created by taozhiheng on 16-8-4.
 */
public class ExclaimBolt extends BaseBasicBolt{
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String input = tuple.getString(1);
        //第一列request请求ID
        collector.emit(new Values(tuple.getValue(0), input + "!"));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "result"));
    }
}
