package com.persist.bolts.image;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.persist.bean.analysis.CalculateInfo;
import com.persist.bean.analysis.ImageInfo;
import com.persist.util.helper.FileHelper;
import com.persist.util.helper.ImageHepler;
import com.persist.util.helper.Logger;
import com.persist.util.tool.analysis.CalculatorImpl;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by taozhiheng on 16-8-3.
 */
public class ImageBolt extends BaseRichBolt {

    private final static String TAG = "ImageBolt";

    private OutputCollector mCollector;
    private CalculatorImpl mCalculator;
    private Gson mGson;

    private int mWidth = 227;
    private int mHeight = 227;

    private String mLib;
    private float mWarnValue;

    public ImageBolt(String lib, float warnValue)
    {
        this.mLib = lib;
        this.mWarnValue = warnValue;
    }

    private ImageBolt(CalculatorImpl calculator)
    {
        mCalculator = calculator;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.mCollector = outputCollector;
        mGson = new Gson();
        if(mCalculator == null)
        {
            mCalculator = CalculatorImpl.getInstance(mLib, mWarnValue);
        }
        mCalculator.prepare();
        //reset log output stream to log file
        try {
            Logger.setOutput(new FileOutputStream("DRPCServer", true));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            Logger.setDebug(false);
        }
    }

    public void execute(Tuple tuple) {
        String data = tuple.getString(0);
        String returnInfo = tuple.getValue(1).toString();
        Logger.log(TAG, "receive data:"+data);
        ImageInfo info = new ImageInfo();
        try
        {
            info = mGson.fromJson(data, ImageInfo.class);
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            if(FileHelper.download(os, info.url))
            {
                Logger.log(TAG, "succeed downloading image form "+info.url);
                InputStream in = new ByteArrayInputStream(os.toByteArray());
                BufferedImage image = ImageIO.read(in);
                if(image.getWidth() != mWidth || image.getHeight() != mHeight)
                    image = ImageHepler.resize(image, mWidth, mHeight);
                byte[] pixels = ((DataBufferByte) image.getRaster().getDataBuffer())
                        .getData();
                List<CalculateInfo> list = new ArrayList<CalculateInfo>(1);
                list.add(new CalculateInfo(info.url, pixels, image.getHeight(), image.getWidth()));
                Logger.log(TAG, "start predict "+info.url);
                HashMap<String, Float> map = mCalculator.predict(list);
                if(map != null && map.size() > 0)
                {
                    for (Map.Entry<String, Float> entry : map.entrySet()) {
                        info.value = entry.getValue();
                        break;
                    }
                    info.ok = info.value < mCalculator.getWarnValue();
                    Logger.log(TAG, "predict " + info.url + " ok, value=" + info.value);
                }
                else
                {
                    Logger.log(TAG, "predict " + info.url + " fail");
                }
            }
            else {
                Logger.log(TAG, "fail downloading image form " + info.url);
            }
        }
        catch (JsonSyntaxException e)
        {
//            e.printStackTrace();
            Logger.log(TAG, "JsonSyntaxException:"+e.getMessage());
        }
        catch (IOException e)
        {
//            e.printStackTrace();
            Logger.log(TAG, "IOException:"+e.getMessage());
        }
        finally
        {
            String result = mGson.toJson(info);
            mCollector.emit(new Values(result, returnInfo));
            mCollector.ack(tuple);
            Logger.log(TAG, "emit "+result);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("result", "result-info"));
    }
}
