package com.persist.bean.image;

/**
 * Created by taozhiheng on 16-8-4.
 *
 *
 *
 */
public class CheckConfig {

    //the drpc function
    public String function = "image-check";
    //the library to predict
    public String so = "CalculatorImpl";
    //the warn value: when a value is more than the warnValue, the image is regarded as unhealthy
    public float warnValue = 0.75f;
    //the image width and height
    public int width = 227;
    public int height = 227;
    //the DrpcSpout parallelism
    public int drpcSpoutParallel = 1;
    //the UrlBolt parallelism
    public int urlBoltParallel = 1;
    //the DownloadBolt parallelism
    public int downloadBoltParallel = 5;
    //the ReturnBolt parallelism
    public int returnBoltParallel = 3;

    //the authentication cache size
    public int authCacheSize = 100;

    //the hbase server hostname or ip
    public String hbaseQuorum = "192.168.0.189";
    //the client port of the zk in hbase server
    public int hbasePort = 2181;
    //the hbase master name, useless
    public String hbaseMater = "tl-P45VJ:60000";
    //the hbase authentication, useless
    public String hbaseAuth = "root";
    //the auth table
    public String hbaseAuthTable = "auth";
    public String hbaseAuthFamily = "info";
    public String hbaseAuthColumn = "password";
    //the usage table
    public String hbaseUsageTable = "usage";
    public String hbaseUsageFamily = "info";
    public String[] hbaseUsageColumns = {"user", "size"};
    //the recent table
    public String hbaseRecentTable = "recent";
    public String hbaseRecentFamily = "info";
    public String[] hbaseRecentColumns = {"urls", "values"};
    public int hbaseRecentCount = 100;

    //the worker number (process number), suggest to set 1,
    //otherwise the gpu resources may be not enough, because each process uses dependent gpu resources
    public int workerNum= 1;

    public CheckConfig()
    {

    }

}
