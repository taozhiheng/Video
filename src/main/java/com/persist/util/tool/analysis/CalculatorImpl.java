package com.persist.util.tool.analysis;

import com.persist.bean.analysis.CalculateInfo;
import com.persist.bean.analysis.PictureKey;
import com.persist.bean.analysis.PictureResult;
import com.persist.util.helper.HDFSHelper;
import com.persist.util.helper.ImageHepler;
import com.persist.util.helper.Logger;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by taozhiheng on 16-7-29.
 *
 * Note:
 * the class name can't not be directly renamed.
 * If you want to rename it, remember to rebuilt the relative .so.
 */
public class CalculatorImpl implements IPictureCalculator {

    private final static String TAG = "CalculatorImpl";

    private float mWarnValue = 0.75f;
    private HDFSHelper mHelper;
    private Map<String, PictureKey> mInfoBuffer;
    private List<CalculateInfo> mBuffer;
    private int mBufferSize = 500;
    private long mDuration = 2000;
    private long lastTime;

    private int mWidth = 227;
    private int mHeight = 227;

    private  static String so;

    private  static boolean hasLibrary = false;

    private static CalculatorImpl INSTANCE;

    public static CalculatorImpl getInstance(String lib)
    {
        return getInstance(lib, 0.75f);
    }

    public static CalculatorImpl getInstance(String lib, float warnValue)
    {
        if(INSTANCE == null)
        {
            synchronized (CalculatorImpl.class)
            {
                if(INSTANCE == null)
                {
                    INSTANCE = new CalculatorImpl(lib, warnValue);
                }
            }
        }
        return INSTANCE;
    }

    private CalculatorImpl(String lib)
    {
        this(lib, 0.75f);
    }

    private CalculatorImpl(String lib, float warnValue)
    {
        if(lib == null)
            throw new RuntimeException("the so must not be null");
        so = lib;
        mWarnValue = warnValue;
        mHelper = new HDFSHelper(null);
        mBuffer = new ArrayList<CalculateInfo>();
        mInfoBuffer = new HashMap<String, PictureKey>();
    }

    public void prepare() {

    }

    public void cleanup() {
        mHelper.close();
    }

    public float getWarnValue()
    {
        return mWarnValue;
    }

    public synchronized List<PictureResult> calculateImage(PictureKey key) {

        if(key == null)
            return null;
        //put data to buffer
        try {
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            if(key.url != null && mHelper.download(os, key.url))
            {
                InputStream in = new ByteArrayInputStream(os.toByteArray());
                BufferedImage image = ImageIO.read(in);
                if (image.getWidth() != mWidth || image.getHeight() != mHeight)
                    image = ImageHepler.resize(image, mWidth, mHeight);
                byte[] pixels = ((DataBufferByte) image.getRaster().getDataBuffer())
                        .getData();
                mInfoBuffer.put(key.url, key);
                mBuffer.add(new CalculateInfo(key.url, pixels, image.getHeight(), image.getWidth()));
            }
            os.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        long curTime = System.currentTimeMillis();
        if(mBuffer.size() >= mBufferSize || ((key.url == null || curTime-lastTime >= mDuration) && mBuffer.size() > 0))
        {
            //calculate
            HashMap<String, Float> map = predict(mBuffer);
            List<PictureResult> results = null;
            if(map != null)
            {
                //return
                results = new ArrayList<PictureResult>(map.size());
                float value;
                boolean ok;
                PictureKey pictureKey;
                for (Map.Entry<String, Float> entry : map.entrySet()) {
                    value = entry.getValue();
                    ok = value < mWarnValue;
                    pictureKey = mInfoBuffer.get(entry.getKey());
                    if(pictureKey == null)
                        pictureKey = new PictureKey(entry.getKey(), "rtmp://unknown", "Unknown");
                    results.add(new PictureResult(pictureKey, ok, value));
                }
            }
            //clear
            mBuffer.clear();
            mInfoBuffer.clear();
            lastTime = curTime;
            return results;
        }
        return null;
    }


    public static synchronized HashMap<String, Float> predict(List<CalculateInfo> images)
    {
        if(!hasLibrary)
        {
            if(so.contains(".so"))
                System.load(so);
            else
                System.loadLibrary(so);
            hasLibrary = true;
        }
        if(images == null || images.size() == 0)
            return null;
        return predict(images, 0, null, null, 102, 0);
    }

    private static native HashMap<String, Float> predict(List<CalculateInfo> images,
                                                  int reset,
                                                  String modelFile,
                                                  String trainedFile,
                                                  int batchSize,
                                                  int gpuId);


}
