package com.persist;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.persist.bean.analysis.AnalysisConfig;
import com.persist.bolts.analysis.PictureNotifierBolt;
import com.persist.bolts.analysis.PictureRecorderBolt;
import com.persist.bolts.analysis.PictureResultBolt;
import com.persist.util.helper.FileHelper;
import com.persist.util.helper.Logger;
import com.persist.util.tool.analysis.*;
import storm.kafka.*;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.Arrays;

/**
 * Created by zhiheng on 2016/7/5.
 *
 */
public class VideoAnalyzer {

    private final static String TAG = "TopologyCreator";

    private final static String KEY_SPOUT = "key-spout";
    private final static String RESULT_BOLT = "result-bolt";
    private final static String NOTIFIER_BOLT = "notifier-bolt";
    private final static String RECORDER_BOLT = "recorder-bolt";

    public static void main(String[] args) throws Exception{

        //default config path
        String configPath = "analyzer_config.json";
        if(args.length > 0)
            configPath = args[0];

        //load config from file "config.json" in current directory
        AnalysisConfig baseConfig = new AnalysisConfig();
        try
        {
            Gson gson = new Gson();
            baseConfig = gson.fromJson(FileHelper.readString(configPath), AnalysisConfig.class);
        }
        catch (JsonSyntaxException e)
        {
            e.printStackTrace();
        }

        //reset log output stream to log file
        try {
            Logger.setOutput(new FileOutputStream(baseConfig.log, true));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            Logger.setDebug(false);
        }

        //construct calculator and notifier to deal with msg
        IPictureCalculator calculator = new PictureCalculatorImpl(
                baseConfig.redisHost, baseConfig.redisPort, baseConfig.redisPassword);
        IPictureNotifier notifier = new PictureNotifierImpl(
                baseConfig.redisHost, baseConfig.redisPort,
                baseConfig.redisPassword, baseConfig.redisChannels);
//        IPictureRecorder recorder = new PictureRecorderImpl(
//                baseConfig.hbaseQuorum, baseConfig.hbasePort,
//                baseConfig.hbaseMater, baseConfig.hbaseAuth,
//                baseConfig.hbaseTable, baseConfig.hbaseColumnFamily,
//                baseConfig.hbaseColumns);
        IPictureRecorder recorder = new PictureRecorderMultipleImpl(
                baseConfig.hbaseQuorum, baseConfig.hbasePort,
                baseConfig.hbaseMater, baseConfig.hbaseAuth,
                baseConfig.hbaseTable, baseConfig.hbaseYellowTable,
                baseConfig.hbaseColumnFamily, baseConfig.hbaseColumns);

        //construct kafka spout config
        BrokerHosts brokerHosts = new ZkHosts(baseConfig.zks);
        SpoutConfig spoutConfig = new SpoutConfig(
                brokerHosts, baseConfig.topic, baseConfig.zkRoot, baseConfig.id);
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConfig.zkServers = Arrays.asList(baseConfig.zkServers);
        spoutConfig.zkPort = baseConfig.zkPort;
        spoutConfig.forceFromStart = false;

        //construct topology builder
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(KEY_SPOUT, new KafkaSpout(spoutConfig), baseConfig.keySpoutParallel);
        builder.setBolt(RESULT_BOLT, new PictureResultBolt(calculator), baseConfig.resultBoltParallel)
                .shuffleGrouping(KEY_SPOUT);
        builder.setBolt(NOTIFIER_BOLT, new PictureNotifierBolt(notifier), baseConfig.notifierBoltParallel)
                .shuffleGrouping(RESULT_BOLT);
        builder.setBolt(RECORDER_BOLT, new PictureRecorderBolt(recorder), baseConfig.recorderBoltParallel)
                .shuffleGrouping(NOTIFIER_BOLT);

        //submit topology
        Config conf = new Config();
        if (args.length > 1) {
            conf.setNumWorkers(3);
            conf.setDebug(false);
            StormSubmitter.submitTopology(args[1], conf, builder.createTopology());
        } else {
            conf.setDebug(true);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("videoAnalyzer", conf, builder.createTopology());
        }

        Logger.close();

    }
}
