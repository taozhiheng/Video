package com.persist.bolts.image;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.persist.bean.analysis.CalculateInfo;
import com.persist.bean.analysis.PictureKey;
import com.persist.util.helper.BufferedImageHelper;
import com.persist.util.helper.FileHelper;
import com.persist.util.helper.FileLogger;
import com.persist.util.helper.ImageHelper;
import com.persist.util.tool.analysis.Predict;
import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.io.*;
import java.util.Map;

/**
 * Created by taozhiheng on 16-8-15.
 *
 * download image from url
 *
 */
public class DownloadBolt extends BaseRichBolt {

    private final static String TAG = "DownloadBolt";

    private OutputCollector mCollector;


    private int mWidth = 227;
    private int mHeight = 227;

    private FileLogger mLogger;
    private int id;
    private long count = 0;

    public DownloadBolt(int width, int height)
    {
        this.mWidth = width;
        this.mHeight = height;
    }

    @Override
    public void cleanup() {
        super.cleanup();
        mLogger.close();
    }

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        mCollector = collector;
        id = context.getThisTaskId();
        mLogger = new FileLogger("download@"+id);
//        System.setProperty("java.awt.headless", "true");
        mLogger.log(TAG+"@"+id, "prepare");
    }

    public void execute(Tuple input) {
        String url = input.getString(0);
        String key = input.getString(1);
        int size = input.getInteger(2);
        String returnInfo = input.getString(3);
        String user = input.getString(4);

        if(url != null)
        {
            try
            {
                ByteArrayOutputStream os = new ByteArrayOutputStream();
                //download image
                if (FileHelper.download(os, url))
                {
                    mLogger.log(TAG + "@" + id, "succeed downloading image from " + url);
                    InputStream in = new ByteArrayInputStream(os.toByteArray());
                    BufferedImage image = ImageIO.read(in);
                    if(image == null)
                    {
                        mLogger.log(TAG+"@"+id, "the image who urls is"+url +" is null!");
                        os.close();
                        mCollector.emit(new Values(key, size, returnInfo, user));
                        mCollector.ack(input);
                        return;
                    }
                    if (image.getWidth() != mWidth || image.getHeight() != mHeight) {
//                        image = ImageHelper.resize(image, mWidth, mHeight);
                        image = BufferedImageHelper.resize(image, mWidth, mHeight);

                    }
                    byte[] pixels = ((DataBufferByte) image.getRaster().getDataBuffer())
                            .getData();
                    PictureKey pictureKey = new PictureKey(url, url, String.valueOf(System.currentTimeMillis()));
                    //put image to prediction image
                    Predict.append(pictureKey, new CalculateInfo(pictureKey.url, pixels, mWidth, mHeight));
                    in.close();
                    count++;
                    mLogger.log(TAG + "@" + id, "append " + url + " ok, total=" + count);

                }
                else
                {
                    mLogger.log(TAG + "@" + id, "fail downloading image from " + url);
                }
                os.close();
            } catch (IOException e)
            {
                e.printStackTrace(mLogger.getPrintWriter());
                mLogger.getPrintWriter().flush();
                //trigger prediction
                mCollector.emit(new Values(key, size, returnInfo, user));
                mCollector.ack(input);
                return;
            }
        }
        //trigger prediction
        mCollector.emit(new Values(key, size, returnInfo, user));
        mCollector.ack(input);
    }


    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("key", "size", "result-info", "user"));
    }
}
