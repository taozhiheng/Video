package com.persist.bean.analysis;

/**
 * Created by taozhiheng on 16-7-12.
 *
 * base config for analysis images
 *
 */
public class AnalysisConfig {

    //the library to predict
    public String so = "";
    //the warn value: when a value is more than the warnValue, the image is regarded as unhealthy
    public float warnValue = 0.75f;
    //the image width and height
    public int width = 227;
    public int height = 227;
    //the image buffer size and timeout duration
    //If there are more bufferSize images or the time from last prediction to now is more than duration,
    //the prediction will be triggered
    public int bufferSize = 1000;
    public long duration = 3000;
    //the tick time to check and flush buffer (time: tick seconds)
    public int tick = 10;

    //the KafkaSpout parallelism which will determine the process num
    public int keySpoutParallel = 1;
    //the PictureResultBolt parallelism
    public int resultBoltParallel = 3;
    //the NotifierBolt parallelism
    public int notifierBoltParallel = 3;
    //the RecorderBolt parallelism
    public int recorderBoltParallel = 3;

    //split multi zks with ','
    public String zks = "127.0.0.1:2181";
    //the top name of the msg from kafka
    public String topic = "topic-capture-image";
    //the zk root dir to store zk data
    public String zkRoot = "/usr/local/kafka_2.11-0.10.0/pyleus-kafka-offsets/video";
    //the kafka consumer id which seems useless
    public String id = "check-image";
    //the zk servers' hostname or ip
    public String[] zkServers;
    //the client port of zk servers
    public int zkPort = 2181;
    //the redis hostname or ip
    public String redisHost = "develop.finalshares.com";
    //the redis client port
    public int redisPort = 6379;
    //the redis password
    public String redisPassword = "redis.2016@develop.finalshares.com";
    //the redis channels which new messages will be published to
    public String[] redisChannels;

    //the hbase server hostname or ip
    public String hbaseQuorum = "192.168.0.189";
    //the client port of the zk in hbase server
    public int hbasePort = 2181;
    //the hbase master name
    public String hbaseMater = "tl-P45VJ:60000";
    //the hbase authentication
    public String hbaseAuth = "root";
    //the hbase table to store all results of images
    public String hbaseTable = "image";
    //the hbase table to store results of images which are unhealthy
    public String hbaseYellowTable = "yellow";
    //the hbase column family of hbaseTable
    public String hbaseColumnFamily = "info";
    //the hbase columns of hbaseColumnFamily
    public String[] hbaseColumns = {"video_id", "time_stamp", "ok", "percent"};
    //the worker number (process number), suggest to set 1,
    //otherwise the gpu resources may be not enough, because each process uses dependent gpu resources
    public int workerNum = 1;

    //the log dir which seems invisible, now it is invalid
    public String log = "image_log";


    public AnalysisConfig()
    {

    }

}
