package com.persist.bolts.image;

import backtype.storm.drpc.DRPCInvocationsClient;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import com.persist.util.helper.Logger;
import org.apache.thrift7.TException;
import org.json.simple.JSONValue;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by taozhiheng on 16-8-4.
 * implement return bolt to replace ReturnResults
 */
public class ReturnBolt extends BaseRichBolt {

    private final static String TAG = "ReturnBolt";

    //record  host:port -> client instance
    private Map<String, DRPCInvocationsClient> mClients;
    private OutputCollector mCollector;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.mCollector = collector;
        mClients = new HashMap<String, DRPCInvocationsClient>();
        //reset log output stream to log file
        try {
            Logger.setOutput(new FileOutputStream("DRPCServer", true));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            Logger.setDebug(false);
        }
    }

    public void execute(Tuple input) {
        String result = (String) input.getValue(0);
        String returnInfo = (String) input.getValue(1);
        if(returnInfo!=null)
        {
            Map retMap = (Map) JSONValue.parse(returnInfo);
            final String host = (String) retMap.get("host");
            final int port = Utils.getInt(retMap.get("port"));
            String id = (String) retMap.get("id");
            String key = host+":"+port;
            DRPCInvocationsClient client;
            if(mClients.containsKey(key))
            {
                client = mClients.get(key);
            }
            else
            {
                client = new DRPCInvocationsClient(host, port);
                mClients.put(key, client);
            }
            try {
                client.result(id, result);
                mCollector.ack(input);
            } catch(TException e) {
                Logger.log(TAG, "Failed to return results to DRPC server:"+e.getMessage());
                mCollector.fail(input);
            }
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
