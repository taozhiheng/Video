package com.persist.bolts.grab;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import clojure.lang.MapEntry;
import com.persist.bean.grab.VideoInfo;
import com.persist.util.helper.FileLogger;
import com.persist.util.helper.ProcessHelper;
import com.persist.util.tool.grab.IGrabber;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Created by taozhiheng on 16-7-15.
 *
 * control grab processes:
 * add, stop, start, destroy
 *
 */
public class GrabBolt extends BaseRichBolt {

    private final static String TAG = "GrabBolt";

    private IGrabber mGrabber;
    private OutputCollector mCollector;
    private int mGrabLimit;
    private int mCurrentGrab;
    //manage process
    private Map<String , Process> mProcessMap;

    private String mRedisHost;
    private int mRedisPort;
    private String mRedisPassword;

    private String mBrokerList;
    private String mSendTopic;


    private FileLogger mLogger;
    private int id;
    private long count = 0;


    public GrabBolt(IGrabber grabber, int grabLimit, String sendTopic, String brokerList)
    {
        this.mGrabber = grabber;
        this.mGrabLimit = grabLimit;
        this.mBrokerList = brokerList;
        this.mSendTopic = sendTopic;
        this.mCurrentGrab = 0;
    }

    public void setRedis(String host, int port, String password)
    {
        this.mRedisHost = host;
        this.mRedisPort = port;
        this.mRedisPassword = password;
    }

    @Override
    public void cleanup() {
        super.cleanup();
        //destroy all children processes
        Process process;
        for(Map.Entry<String, Process> item : mProcessMap.entrySet())
        {
            process = item.getValue();
            if (process != null)
            {
                ProcessHelper.sendMessage(process, VideoInfo.DEL);
                ProcessHelper.finishMessage(process);
                process.destroy();
                mCurrentGrab--;
                mLogger.log(TAG + "@" + id, "*cleanup*, destroy process:" + process);
            }
        }
        mProcessMap.clear();
        mLogger.close();
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        mCollector = outputCollector;
        mProcessMap = new HashMap<String, Process>(mGrabLimit);
        mCurrentGrab = 0;
        id = topologyContext.getThisTaskId();
        mLogger = new FileLogger("grab@"+id);
        mLogger.log(TAG+"@"+id, "prepare, current process status:"+mCurrentGrab+"/"+mGrabLimit);
    }

    /**
     * grab video and store key frame pictures, and emit the tuple(url, process)
     * or
     * emit the tuple(url, null)
     * */
    public void execute(Tuple tuple) {
        VideoInfo videoInfo = (VideoInfo) tuple.getValue(1);
        if(videoInfo == null)
            return;
        count++;
        Process process = null;
        mLogger.log(TAG+"@"+id, "grab data: "+videoInfo.cmd+","+videoInfo.url+","+videoInfo.dir);
        //add
        if(videoInfo.cmd.equals(VideoInfo.ADD))
        {
            //if there too many running child processes, fail
            if (mCurrentGrab >= mGrabLimit)
            {
                mLogger.log(TAG+"@"+id, "child process num has been max value:"+mCurrentGrab+"/"+mGrabLimit);
                mCollector.fail(tuple);
                return;
            }
            process = mProcessMap.get(videoInfo.url);
            if(process != null)
            {
                try {
                    boolean hasExit = process.waitFor(-1, TimeUnit.SECONDS);
                    //the process is still alive, ignore the message
                    if(!hasExit)
                    {
                        mLogger.log(TAG+"@"+id, "the url:"+videoInfo.url+" is being grabbed!");
                        mCollector.ack(tuple);
                        return;
                    }
                    //the process has exit, remove the process record
                    else
                    {
                        mProcessMap.remove(videoInfo.url);
                        mCurrentGrab--;
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace(mLogger.getPrintWriter());
                    mLogger.getPrintWriter().flush();
                    mCollector.ack(tuple);
                    return;
                }
            }
            //grab and send message to kafka
            process = mGrabber.grab(mRedisHost, mRedisPort, mRedisPassword,
                    videoInfo.url, videoInfo.dir,
                    mSendTopic, mBrokerList);
            if(process != null)
            {
                mProcessMap.put(videoInfo.url, process);
                mCurrentGrab++;
                mLogger.log(TAG+"@"+id, "start process:"+process);
            }
        }
        //delete
        else if(videoInfo.cmd.equals(VideoInfo.DEL))
        {
            process = mProcessMap.get(videoInfo.url);
            if(process != null) {
                ProcessHelper.sendMessage(process, VideoInfo.DEL);
                ProcessHelper.finishMessage(process);
                process.destroy();
                mProcessMap.remove(videoInfo.url);
                mCurrentGrab--;
                mLogger.log(TAG+"@"+id, "destroy process:"+process);
            }
        }
        //pause
        else if(videoInfo.cmd.equals(VideoInfo.PAUSE))
        {
            process = mProcessMap.get(videoInfo.url);
            if(process != null)
            {
                ProcessHelper.sendMessage(process, VideoInfo.PAUSE);
                mLogger.log(TAG+"@"+id, "pause process:"+process);
            }
        }
        //continue
        else if(videoInfo.cmd.equals(VideoInfo.CONTINUE))
        {
            process = mProcessMap.get(videoInfo.url);
            if(process != null)
            {
                ProcessHelper.sendMessage(process, VideoInfo.CONTINUE);
                mLogger.log(TAG+"@"+id, "continue process:"+process);
            }
        }
        mLogger.log(TAG, "child process num: "+mCurrentGrab+"/"+mGrabLimit+", msg total="+count);
        mCollector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }
}
