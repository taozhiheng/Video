package com.persist;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.drpc.DRPCSpout;
import backtype.storm.topology.TopologyBuilder;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.persist.bean.image.CheckConfig;
import com.persist.bolts.image.DownloadBolt;
import com.persist.bolts.image.UrlBolt;
import com.persist.bolts.image.PredictBolt;
import com.persist.bolts.image.ReturnBolt;
import com.persist.util.helper.FileHelper;
import com.persist.util.helper.Logger;
import com.persist.util.tool.image.AuthImpl;
import com.persist.util.tool.image.IAuth;
import com.persist.util.tool.image.IRecorder;
import com.persist.util.tool.image.RecorderImpl;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;

/**
 * Created by taozhiheng on 16-8-3.
 *
 * build and submit a drpc topology to analysis pictures
 *
 * Note:
 * "storm drpc" should be executed  to start drpc service before create the topology
 *
 */
public class ImageCheck {

    private final static String TAG = "ImageCheck";

    private final static String INPUT_SPOUT = "input-spout";
    private final static String URL_BOLT = "url_bolt";
    private final static String DOWNLOAD_BOLT = "download-bolt";
    private final static String PREDICT_BOLT = "predict_bolt";
    private final static String RETURN_BOLT = "return-bolt";

    public static void main(String[] args) throws Exception
    {

        //reset log output stream to log file
        try {
            Logger.setOutput(new FileOutputStream("ImageCheck", true));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            Logger.setDebug(false);
        }

        //default config path
        String configPath = "check.json";
        if(args.length > 0)
            configPath = args[0];

        //load config from file "config.json" in current directory
        CheckConfig baseConfig = new CheckConfig();
        Gson gson = new Gson();
        try
        {
            baseConfig = gson.fromJson(FileHelper.readString(configPath), CheckConfig.class);
        }
        catch (JsonSyntaxException e)
        {
            e.printStackTrace();
        }

        Logger.log(TAG, "configPath:"+configPath);
        Logger.log(TAG, gson.toJson(baseConfig));
        //create auth tool
        IAuth auth = new AuthImpl(
                baseConfig.hbaseQuorum, baseConfig.hbasePort,
                baseConfig.hbaseMater, baseConfig.hbaseAuth,
                baseConfig.hbaseAuthTable, baseConfig.hbaseAuthFamily,
                baseConfig.hbaseAuthColumn, baseConfig.authCacheSize);
        //create record tool
        IRecorder recorder = new RecorderImpl(
                baseConfig.hbaseQuorum, baseConfig.hbasePort,
                baseConfig.hbaseMater, baseConfig.hbaseAuth,
                baseConfig.hbaseUsageTable, baseConfig.hbaseUsageFamily,
                baseConfig.hbaseUsageColumns,
                baseConfig.hbaseRecentTable, baseConfig.hbaseRecentFamily,
                baseConfig.hbaseRecentColumns);

        TopologyBuilder builder = new TopologyBuilder();
        DRPCSpout drpcSpout = new DRPCSpout(baseConfig.function);
        builder.setSpout(INPUT_SPOUT, drpcSpout, baseConfig.drpcSpoutParallel);
        builder.setBolt(URL_BOLT, new UrlBolt(), baseConfig.urlBoltParallel)
                .shuffleGrouping(INPUT_SPOUT);
        builder.setBolt(DOWNLOAD_BOLT, new DownloadBolt(baseConfig.width, baseConfig.height),
                baseConfig.downloadBoltParallel)
                .shuffleGrouping(URL_BOLT);
        builder.setBolt(PREDICT_BOLT, new PredictBolt(baseConfig.so, baseConfig.warnValue), 1)
                .shuffleGrouping(DOWNLOAD_BOLT);
        builder.setBolt(RETURN_BOLT, new ReturnBolt(recorder, baseConfig.hbaseRecentCount), baseConfig.returnBoltParallel)
                .shuffleGrouping(PREDICT_BOLT);
//        builder.setBolt(RETURN_BOLT, new ReturnResults(), 3)
//                .shuffleGrouping(IMAGE_BOLT);


        Config conf = new Config();

        if (args.length > 1) {
            conf.setDebug(false);
            conf.setNumWorkers(baseConfig.workerNum);
            StormSubmitter.submitTopology(args[1], conf, builder.createTopology());
            Logger.log(TAG, "submit remote topology: "+args[1]+", function is: "+baseConfig.function);
        } else {
            conf.setDebug(true);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("ImageCheck", conf, builder.createTopology());
            Logger.log(TAG, "submit local topology: ImageCheck, function is: "+baseConfig.function);
        }
        Logger.close();

    }
}
