package com.persist.bean.grab;

/**
 * Created by taozhiheng on 16-7-12.
 *
 * base config for grab frames from video(rtmp)
 *
 */
public class GrabConfig {

    //the KafkaSpout parallelism which will determine the process num
    public int urlSpoutParallel = 1;
    //the resolveBolt parallelism
    public int resolveBoltParallel = 3;
    //the GrabBolt parallelism
    public int grabBoltParallel = 3;

    //the max child process num to grab pictures from video
    public int grabLimit = 60;
    //the command of the grab frames with executable process)
    public String cmd = "storm jar $STORM_HOME/Video.jar com.persist.GrabThread ";

    //split multi zk with ','
    public String zks = "zk01:2181,zk02:2181,zk03:2181/kafka";
    //the top name of the msg from kafka
    public String topic = "topic-capture-image";
    //the zk root dir to store zk data
    public String zkRoot = "/storm/grab";
    //the kafka consumer id which seems useless
    public String id = "consumer-grab";
    //the zk servers' hostname or ip
    public String[] zkServers;
    //the client port of zk servers
    public int zkPort = 2181;

    public String brokerList = "storm01:9092,storm02:9092,storm03:9092";

    public String sendTopic = "topic-check-images-can-ignore";

    //the redis hostname or ip
    public String redisHost = "develop.finalshares.com";
    //the redis client port
    public int redisPort = 6379;
    //the redis password
    public String redisPassword = "redis.2016@develop.finalshares.com";

    //the format to name pictures
    public String nameFormat = "frame-%05d-%d.png";

    //the frame rate to grab
    public double frameRate = 1.0;

    //the num to workers
    public int workerNum = 3;

    //the log dir which seems invisible
    public String log = "video_log";

    public GrabConfig()
    {

    }

}
