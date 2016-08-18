package com.persist.bolts.image;

import backtype.storm.drpc.DRPCInvocationsClient;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import com.persist.util.helper.FileLogger;
import org.apache.thrift7.TException;
import org.json.simple.JSONValue;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by taozhiheng on 16-8-4.
 *
 * implement return bolt to replace ReturnResults
 *
 * return results to client
 *
 */
public class ReturnBolt extends BaseRichBolt {

    private final static String TAG = "ReturnBolt";

    //record  host:port -> client instance
    private Map<String, DRPCInvocationsClient> mClients;
    private OutputCollector mCollector;

    private FileLogger mLogger;
    private int id;
    private long count = 0;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.mCollector = collector;
        mClients = new HashMap<String, DRPCInvocationsClient>();

        id = context.getThisTaskId();
        mLogger = new FileLogger("return@"+id);
        mLogger.log(TAG+"@"+id, "prepare");

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
            //get client from buffer
            if(mClients.containsKey(key))
            {
                client = mClients.get(key);
            }
            //create new client
            else
            {
                client = new DRPCInvocationsClient(host, port);
                mClients.put(key, client);
            }
            try {
                //return response message to client
                client.result(id, result);
                mLogger.log(TAG+"@"+this.id, "Succeed to return results: "+result);
                count++;
                mLogger.log(TAG+"@"+this.id, "total="+count);
                mCollector.ack(input);
            } catch(TException e) {
                e.printStackTrace(mLogger.getPrintWriter());
                mLogger.getPrintWriter().flush();
                mCollector.fail(input);
            }
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
