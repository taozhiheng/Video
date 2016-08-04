package com.persist.bolts.grab;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.persist.bean.grab.VideoInfo;
import com.persist.util.helper.Logger;
import com.persist.util.helper.ProcessHelper;
import com.persist.util.tool.grab.IGrabber;
import com.persist.util.tool.grab.IVideoNotifier;
import com.persist.util.tool.grab.VideoNotifierImpl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by taozhiheng on 16-7-15.
 * GrabBolt will receive two types tuple:
 * First - grab:
 * Try to start a child process
 * to grab key frame pictures from video and store them,
 * and emit the tuple(url, process) to KillBolt
 * so that KillBolt can record the child process
 * Second - kill
 * emit the tuple(url, null) to KillBolt
 * so that KillBolt can kill the process specified by url
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
        Logger.close();
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        mCollector = outputCollector;
        mProcessMap = new HashMap<String, Process>(mGrabLimit);
        mCurrentGrab = 0;
        try {
            Logger.setOutput(new FileOutputStream("VideoGrabber", true));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            Logger.setDebug(false);
        }
        Logger.log(TAG, "prepare, current process status:"+mCurrentGrab+"/"+mGrabLimit);
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
        Process process = null;
        Logger.log(TAG, "grab data: "+videoInfo.cmd+","+videoInfo.url+","+videoInfo.dir);
        //add
        if(videoInfo.cmd.equals(VideoInfo.ADD))
        {
            //if there too many running child processes, fail
            if (mCurrentGrab >= mGrabLimit)
            {
                Logger.log(TAG, "child process num has been max value:"+mCurrentGrab+"/"+mGrabLimit);
                mCollector.fail(tuple);
                return;
            }
            if(mProcessMap.get(videoInfo.url) != null)
            {
                mCollector.ack(tuple);
                Logger.log(TAG, "the url:"+videoInfo.url+" is being grabbed!");
                return;
            }
            //grab and send message to kafka
            process = mGrabber.grab(mRedisHost, mRedisPort, mRedisPassword,
                    videoInfo.url, videoInfo.dir,
                    mSendTopic, mBrokerList);
            if(process != null)
            {
                mProcessMap.put(videoInfo.url, process);
                mCurrentGrab++;
                Logger.log(TAG, "start process:"+process);
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
                Logger.log(TAG, "destroy process:"+process);
            }
        }
        //pause
        else if(videoInfo.cmd.equals(VideoInfo.PAUSE))
        {
            process = mProcessMap.get(videoInfo.url);
            if(process != null)
            {
                ProcessHelper.sendMessage(process, VideoInfo.PAUSE);
                Logger.log(TAG, "pause process:"+process);
            }
        }
        //continue
        else if(videoInfo.cmd.equals(VideoInfo.CONTINUE))
        {
            process = mProcessMap.get(videoInfo.url);
            if(process != null)
            {
                ProcessHelper.sendMessage(process, VideoInfo.CONTINUE);
                Logger.log(TAG, "continue process:"+process);
            }
        }
        Logger.log(TAG, "child process num: "+mCurrentGrab+"/"+mGrabLimit);
        mCollector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
//        outputFieldsDeclarer.declare(new Fields("src", "process"));
    }
}
