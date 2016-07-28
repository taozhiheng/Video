package com.persist;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.persist.bean.grab.GrabConfig;
import com.persist.bolts.grab.GrabBolt;
import com.persist.bolts.grab.ResolveBolt;
import com.persist.spouts.MyKafkaSpout;
import com.persist.util.helper.FileHelper;
import com.persist.util.helper.Logger;
import com.persist.util.tool.grab.GrabberImpl;
import com.persist.util.tool.grab.IGrabber;
import storm.kafka.*;
import sun.rmi.runtime.Log;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.Arrays;

/**
 * Created by taozhiheng on 16-7-15.
 *
 */
public class VideoGrabber {

    private final static String TAG = "VideoGrabber";

    private final static String URL_SPOUT = "url-spout";
    private final static String RESOLVE_BOLT = "resolve-bolt";
    private final static String GRAB_BOLT = "grab-bolt";
//    private final static String KILL_BOLT = "kill-bolt";

    public static void main(String[] args) throws Exception
    {

        try {
            Logger.setOutput(new FileOutputStream(TAG, true));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            Logger.setDebug(false);
        }

        String configPath = "grabber_config.json";
        if(args.length > 0)
            configPath = args[0];

        Logger.log(TAG, "configPath:"+configPath);
        //load config from file "config.json" in current directory
        GrabConfig grabConfig = new GrabConfig();
        try
        {
            Gson gson = new Gson();
            grabConfig = gson.fromJson(FileHelper.readString(configPath), GrabConfig.class);
        }
        catch (JsonSyntaxException e)
        {
            e.printStackTrace();
        }
        //reset log output stream to log file


        //construct kafka spout config
        BrokerHosts brokerHosts = new ZkHosts(grabConfig.zks);
        SpoutConfig spoutConfig = new SpoutConfig(
                brokerHosts, grabConfig.topic, grabConfig.zkRoot, grabConfig.id);
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConfig.zkServers = Arrays.asList(grabConfig.zkServers);
        spoutConfig.zkPort = grabConfig.zkPort;
        spoutConfig.forceFromStart = false;

        IGrabber grabber = new GrabberImpl(grabConfig.cmd, grabConfig.nameFormat);

        //construct topology builder
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(URL_SPOUT, new KafkaSpout(spoutConfig), grabConfig.urlSpoutParallel);
//        builder.setSpout(URL_SPOUT,
//                new MyKafkaSpout(grabConfig.zks, grabConfig.topic, grabConfig.zkRoot, grabConfig.id), grabConfig.urlSpoutParallel);
        Logger.log(TAG, URL_SPOUT);
        Logger.log(TAG, "zks:"+grabConfig.zks+", topic:"+grabConfig.topic+", zkRoot:"+grabConfig.zkRoot+", id:"+grabConfig.id);


        builder.setBolt(RESOLVE_BOLT,
                new ResolveBolt(), grabConfig.resolveBoltParallel)
                .shuffleGrouping(URL_SPOUT);
        GrabBolt grabBolt = new GrabBolt(grabber, grabConfig.grabLimit/grabConfig.grabBoltParallel,
                grabConfig.sendTopic, grabConfig.brokerList);
        grabBolt.setRedis(grabConfig.redisHost, grabConfig.redisPort, grabConfig.redisPassword);
        builder.setBolt(GRAB_BOLT,
                grabBolt,
                grabConfig.grabBoltParallel)
                .fieldsGrouping(RESOLVE_BOLT, new Fields("url"));


        //submit topology
        Config conf = new Config();
        if (args.length > 1) {
            conf.setNumWorkers(3);
            conf.setDebug(false);
            StormSubmitter.submitTopology(args[1], conf, builder.createTopology());
        } else {
            conf.setDebug(true);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("videoGrabber", conf, builder.createTopology());
        }

        Logger.close();

    }
}