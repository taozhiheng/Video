package com.persist.bean.analysis;

/**
 * Created by taozhiheng on 16-7-12.
 * base config:
 * parallelism config
 * zookeeper and storm config
 * redis server config
 * hbase config
 * log config
 */
public class AnalysisConfig {

    public String so = "";

    public float warnValue = 0.75f;

    //the KafkaSpout parallelism which will determine the process num
    public int keySpoutParallel = 1;
    //the PictureResultBolt parallelism
    public int resultBoltParallel = 3;
    //the NotifierBolt parallelism
    public int notifierBoltParallel = 3;
    //the RecorderBolt parallelism
    public int recorderBoltParallel = 3;

    //split multi zk with ','
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

    public String hbaseTable = "image";

    public String hbaseYellowTable = "yellow";

    public String hbaseColumnFamily = "info";

    public String[] hbaseColumns = {"video_id", "time_stamp", "ok", "percent"};

    //the num to workers
    public int workerNum = 3;

    //the log dir which seems invisible
    public String log = "image_log";


    public AnalysisConfig()
    {

    }

}
