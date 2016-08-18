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
import com.persist.util.helper.FileHelper;
import com.persist.util.helper.Logger;
import com.persist.util.tool.grab.GrabberImpl;
import com.persist.util.tool.grab.IGrabber;
import storm.kafka.*;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.Arrays;

/**
 * Created by taozhiheng on 16-7-15.
 *
 * build and submit a topology to grab videos
 *
 */
public class VideoGrabber {

    private final static String TAG = "VideoGrabber";

    private final static String URL_SPOUT = "url-spout";
    private final static String RESOLVE_BOLT = "resolve-bolt";
    private final static String GRAB_BOLT = "grab-bolt";

    public static void main(String[] args) throws Exception
    {
        //reset log output stream to log file
        try {
            Logger.setOutput(new FileOutputStream(TAG, true));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            Logger.setDebug(false);
        }

        String configPath = "grabber_config.json";
        if(args.length > 0)
            configPath = args[0];

        //load config from file "config.json" in current directory
        GrabConfig baseConfig = new GrabConfig();
        Gson gson = new Gson();
        try
        {
            baseConfig = gson.fromJson(FileHelper.readString(configPath), GrabConfig.class);
        }
        catch (JsonSyntaxException e)
        {
            e.printStackTrace();
        }

        Logger.log(TAG, "configPath:"+configPath);
        Logger.log(TAG, gson.toJson(baseConfig));


        //construct kafka spout config
        BrokerHosts brokerHosts = new ZkHosts(baseConfig.zks);
        SpoutConfig spoutConfig = new SpoutConfig(
                brokerHosts, baseConfig.topic, baseConfig.zkRoot, baseConfig.id);
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConfig.zkServers = Arrays.asList(baseConfig.zkServers);
        spoutConfig.zkPort = baseConfig.zkPort;
        spoutConfig.forceFromStart = false;

        IGrabber grabber = new GrabberImpl(baseConfig.cmd, baseConfig.nameFormat, baseConfig.frameRate);

        //construct topology builder
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(URL_SPOUT, new KafkaSpout(spoutConfig), baseConfig.urlSpoutParallel);


        builder.setBolt(RESOLVE_BOLT,
                new ResolveBolt(), baseConfig.resolveBoltParallel)
                .shuffleGrouping(URL_SPOUT);
        GrabBolt grabBolt = new GrabBolt(grabber, baseConfig.grabLimit/baseConfig.grabBoltParallel,
                baseConfig.sendTopic, baseConfig.brokerList);
        grabBolt.setRedis(baseConfig.redisHost, baseConfig.redisPort, baseConfig.redisPassword);
        builder.setBolt(GRAB_BOLT,
                grabBolt,
                baseConfig.grabBoltParallel)
                .fieldsGrouping(RESOLVE_BOLT, new Fields("url"));


        //submit topology
        Config conf = new Config();
        if (args.length > 1) {
            conf.setNumWorkers(baseConfig.workerNum);
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
