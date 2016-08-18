package com.persist.spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.persist.kafka.KafkaHighLevelConsumer;
import java.util.List;
import java.util.Map;

/**
 * Created by taozhiheng on 16-7-28.
 *
 */
public class MyKafkaSpout extends BaseRichSpout {

    private KafkaHighLevelConsumer mConsumer;
    private String mBrokers;
    private String mTopic;
    private String mGroupId;
    private String mClientId;

    private SpoutOutputCollector mCollector;


    public MyKafkaSpout(String brokers, String topic, String groupId, String clientId)
    {
        this.mBrokers = brokers;
        this.mTopic = topic;
        this.mGroupId = groupId;
        this.mClientId = clientId;
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("msg"));
    }

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.mCollector = spoutOutputCollector;
        mConsumer = new KafkaHighLevelConsumer("kafka01:9092,kafka02:9092,kafka03:9092", "topic-capture-image", "/storm/grab", "consumer-grab");
    }

    public void nextTuple() {
        List<String> messages = mConsumer.getMessage(1);
        for(String msg : messages)
        {
            mCollector.emit(new Values(msg));
        }
    }
}
