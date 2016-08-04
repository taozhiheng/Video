package com.persist;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.drpc.DRPCSpout;
import backtype.storm.topology.TopologyBuilder;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.persist.bean.image.CheckConfig;
import com.persist.bolts.image.ImageBolt;
import com.persist.bolts.image.ReturnBolt;
import com.persist.util.helper.FileHelper;
import com.persist.util.helper.Logger;
import com.persist.util.tool.analysis.CalculatorImpl;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;

/**
 * Created by taozhiheng on 16-8-3.
 *
 */
public class ImageCheck {

    private final static String TAG = "ImageCheck";

    private final static String INPUT_SPOUT = "input-spout";
    private final static String DEAL_BOLT = "deal_bolt";
    private final static String RETURN_BOLT = "return-bolt";

    public static void main(String[] args) throws Exception
    {

        //reset log output stream to log file
        try {
            Logger.setOutput(new FileOutputStream("DRPCServer", true));
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
        try
        {
            Gson gson = new Gson();
            baseConfig = gson.fromJson(FileHelper.readString(configPath), CheckConfig.class);
        }
        catch (JsonSyntaxException e)
        {
            e.printStackTrace();
        }
        TopologyBuilder builder = new TopologyBuilder();

        CalculatorImpl calculator = new CalculatorImpl(baseConfig.so);

        DRPCSpout drpcSpout = new DRPCSpout(baseConfig.function);
        builder.setSpout(INPUT_SPOUT, drpcSpout, baseConfig.drpcSpoutParallel);
        builder.setBolt(DEAL_BOLT, new ImageBolt(calculator), baseConfig.imageBoltParallel)
                .shuffleGrouping(INPUT_SPOUT);
        builder.setBolt(RETURN_BOLT, new ReturnBolt(), baseConfig.returnBoltParallel)
                .shuffleGrouping(DEAL_BOLT);
//        builder.setBolt(RETURN_BOLT, new ReturnResults(), 3)
//                .shuffleGrouping(DEAL_BOLT);

        Logger.log(TAG, "create drpc topology, function is image-check");

        Config conf = new Config();

        if (args.length > 1) {
            conf.setDebug(false);
            conf.setNumWorkers(baseConfig.workerNum);
            StormSubmitter.submitTopology(args[1], conf, builder.createTopology());

        } else {
            conf.setDebug(true);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("image-check", conf, builder.createTopology());

//            LocalDRPC drpc = new LocalDRPC();
//            for (String word : new String[]{"hello", "goodbye"}) {
//                Logger.log(TAG, "Result for \"" + word + "\": " + drpc.execute("image-check", word));
//            }
//            Logger.log(TAG, "submit local topology and test");
//            cluster.shutdown();
//            drpc.shutdown();
        }
    }
}
