package com.persist.bolts.analysis;

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
import com.persist.bean.analysis.PictureKey;
import com.persist.util.helper.FileLogger;
import com.persist.util.helper.HDFSHelper;
import com.persist.util.helper.ImageHepler;
import com.persist.util.tool.analysis.Predict;
import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.io.*;
import java.util.Map;

/**
 * Created by zhiheng on 2016/7/5.
 *
 * resolve image info from json string,
 * download image form url,
 * try to trigger PictureCalculateBolt to predict
 *
 */
public class PictureResultBolt extends BaseRichBolt {

    private final static String TAG = "PictureResultBolt";

    private OutputCollector mCollector;
    private Gson mGson;
    private HDFSHelper mHelper;
    private int mWidth = 227;
    private int mHeight = 227;

    private FileLogger mLogger;
    private int id;
    private long count = 0;

    public PictureResultBolt(int width, int height)
    {
        this.mWidth = width;
        this.mHeight = height;
    }


    @Override
    public void cleanup() {
        super.cleanup();
        mHelper.close();
        mLogger.close();
    }

    /**
     * init collector
     * and init calculator, actually init redis
     * */
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.mCollector = outputCollector;
        this.mGson = new Gson();
        mHelper = new HDFSHelper(null);
        id = topologyContext.getThisTaskId();
        mLogger = new FileLogger("picture-result@"+id);
        mLogger.log(TAG+"@"+id, "prepare");
    }

    /**
     * resolve the msg from string to json to object
     * and emit result to PictureRecorderBolt
     * */
    public void execute(Tuple tuple) {
        String data = tuple.getString(0);
        mLogger.log(TAG+"@"+id, "receive kafka msg: "+data);
        PictureKey pictureKey;
        try {
            //resolve json string
            pictureKey = mGson.fromJson(data, PictureKey.class);
            if(pictureKey != null)
            {
                try {
                    ByteArrayOutputStream os = new ByteArrayOutputStream();
                    //download image
                    if(pictureKey.url != null && mHelper.download(os, pictureKey.url))
                    {
                        InputStream in = new ByteArrayInputStream(os.toByteArray());
                        BufferedImage image = ImageIO.read(in);
                        if (image.getWidth() != mWidth || image.getHeight() != mHeight)
                            image = ImageHepler.resize(image, mWidth, mHeight);
                        byte[] pixels = ((DataBufferByte) image.getRaster().getDataBuffer())
                                .getData();
                        //put image to prediction buffer
                        boolean ready = Predict.append(pictureKey, new CalculateInfo(pictureKey.url,pixels, mWidth, mHeight));
                        count++;
                        mLogger.log(TAG+"@"+id, "append "+pictureKey.url+" ok, ready="+ready+", total="+count);
                        if(ready)
                        {
                            //trigger prediction
                            mCollector.emit(new Values(true));
                        }
                    }
                    os.close();
                } catch (IOException e) {
                    e.printStackTrace(mLogger.getPrintWriter());
                    mLogger.getPrintWriter().flush();
                }
            }
        }
        catch (JsonSyntaxException e)
        {
            e.printStackTrace(mLogger.getPrintWriter());
            mLogger.getPrintWriter().flush();
        }
        finally
        {
            mCollector.ack(tuple);
        }

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("signal"));
    }
}
