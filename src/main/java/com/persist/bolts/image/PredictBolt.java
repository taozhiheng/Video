package com.persist.bolts.image;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.gson.Gson;
import com.persist.bean.analysis.PictureResult;
import com.persist.bean.image.ImageInfo;
import com.persist.bean.image.UrlInfo;
import com.persist.util.helper.FileLogger;
import com.persist.util.tool.analysis.Predict;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by taozhiheng on 16-8-12.
 *
 * load .so library from lib or absolute path,
 * predict images,
 * and trigger ReturnBolt to return results
 *
 */
public class PredictBolt extends BaseRichBolt{

    private final static String TAG = "PredictBolt";

    private final static float DEFAULT_WARN_VALUE = 0.75f;
    private final static int DEFAULT_BUFFER_SIZE = 1;
    private final static int DEFAULT_DURATION = 3000;

    private OutputCollector mCollector;

    private Map<String, UrlInfo> mUrlMap;

    private FileLogger mLogger;
    private int id;
    private long count = 0;
//    private Gson mGson;

    private String so;
    private float warnValue;
    private int bufferSize;
    private long duration;

    public PredictBolt(String so)
    {
        this(so, DEFAULT_WARN_VALUE);
    }

    public PredictBolt(String so, float warnValue)
    {
        this(so, warnValue, DEFAULT_BUFFER_SIZE, DEFAULT_DURATION);
    }

    public PredictBolt(String so, float warnValue, int bufferSize, long duration)
    {
        this.so = so;
        this.warnValue = warnValue;
        this.bufferSize = bufferSize;
        this.duration = duration;
    }

    @Override
    public void cleanup() {
        super.cleanup();
        mLogger.close();
    }

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        mCollector = collector;
        Predict.init(so);
        Predict.setWarnValue(warnValue);
        Predict.setBufferSize(bufferSize);
        Predict.setDuration(duration);
//        mGson = new Gson();
        mUrlMap = new HashMap<String, UrlInfo>();
        id = context.getThisTaskId();
        mLogger = new FileLogger("triggerPredict@"+id);
        mLogger.log(TAG+"@"+id, "prepare");
    }

    public void execute(Tuple input) {
        String key = input.getString(0);
        int size = input.getInteger(1);
        String returnInfo = input.getString(2);
        String user = input.getString(3);

        UrlInfo urlInfo = mUrlMap.get(key);
        //if the urlInfo don't exist, put it to mUrlMap
        if(urlInfo == null)
        {
            urlInfo = new UrlInfo(0, size);
            mUrlMap.put(key, urlInfo);
        }
        urlInfo.count++;
        //count is not enough, there are still download tasks unfinished
        if(urlInfo.count < size)
        {
            mCollector.ack(input);
            return;
        }

        //trigger predict event
        mLogger.log(TAG+"@"+id, "trigger triggerPredict, size="+size);
        long start = System.currentTimeMillis();
        List<PictureResult> list = Predict.triggerPredict(true);
        long end = System.currentTimeMillis();
        //construct response messages to client
        ImageInfo info = null;
        if (list != null && list.size() > 0)
        {
            String[] urls = new String[list.size()];
            float[] values = new float[list.size()];
            boolean[] oks = new boolean[list.size()];
            int i = 0;
            for (PictureResult result : list)
            {
                urls[i] = result.description.url;
                values[i] = result.percent;
                oks[i] = result.ok;
                i++;
            }
            count += list.size();
            mLogger.log(TAG+"@"+id, "size="+list.size()+", time="+(end-start)+", total="+count);
            info = new ImageInfo(urls, values, oks);
//            String result = mGson.toJson(info);
        }
        mCollector.emit(new Values(info, returnInfo, user));
        //remove urlInfo from mUrlMap
        mUrlMap.remove(key);
        mCollector.ack(input);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("result", "result-info", "user"));
    }
}
