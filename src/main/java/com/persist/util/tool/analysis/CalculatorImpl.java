package com.persist.util.tool.analysis;

import com.persist.bean.analysis.CalculateInfo;
import com.persist.bean.analysis.PictureKey;
import com.persist.bean.analysis.PictureResult;
import com.persist.util.helper.HDFSHelper;
import com.persist.util.helper.ImageHepler;
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

    private float mWarnValue = 0.75f;
    private HDFSHelper mHelper;
    private Map<String, PictureKey> mInfoBuffer;
    private List<CalculateInfo> mBuffer;
    private int mBufferSize = 100;
    private long mDuration = 2000;
    private long lastTime;

    private int mWidth = 227;
    private int mHeight = 227;

    private String so;


    public CalculatorImpl(String so)
    {
        this(so, 0.75f);
    }

    public CalculatorImpl(String so, float warnValue)
    {
        if(so == null)
            throw new RuntimeException("the so must not be null");
        this.so = so;
        mWarnValue = warnValue;
        mHelper = new HDFSHelper(null);
        mBuffer = new ArrayList<CalculateInfo>();
        mInfoBuffer = new HashMap<String, PictureKey>();
    }

    public void prepare() {
        if(so.contains(".so"))
            System.load(so);
        else
            System.loadLibrary(so);
    }

    public float getWarnValue()
    {
        return mWarnValue;
    }

    public List<PictureResult> calculateImage(PictureKey key) {

        //put data to buffer
        try {
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            mHelper.download(os, key.url);
            InputStream in = new ByteArrayInputStream(os.toByteArray());
            BufferedImage image = ImageIO.read(in);
            if(image.getWidth() != mWidth || image.getHeight() != mHeight)
                image = ImageHepler.resize(image, mWidth, mHeight);
            byte[] pixels = ((DataBufferByte) image.getRaster().getDataBuffer())
                    .getData();
            mBuffer.add(new CalculateInfo(key.url, pixels, image.getHeight(), image.getWidth()));
            mInfoBuffer.put(key.url, key);
            os.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        long curTime = System.currentTimeMillis();
        if(mBuffer.size() >= mBufferSize || (curTime-lastTime >= mDuration && mBuffer.size() > 0))
        {
            //calculate
            HashMap<String, Float> map = predict(mBuffer);
            //return
            List<PictureResult> results = new ArrayList<PictureResult>(map.size());
            float value;
            boolean ok;
            for(Map.Entry<String, Float> entry : map.entrySet())
            {
                value = entry.getValue();
                ok = value < mWarnValue;
                results.add(new PictureResult(mInfoBuffer.get(entry.getKey()), ok, value));
            }
            //clear
            mBuffer.clear();
            mInfoBuffer.clear();
            lastTime = curTime;
            return results;
        }
        return null;
    }

//    // Convert image to Mat
//    public static Mat matify(BufferedImage im) {
//        // Convert INT to BYTE
//        //im = new BufferedImage(im.getWidth(), im.getHeight(),BufferedImage.TYPE_3BYTE_BGR);
//        // Convert bufferedimage to byte array
//        byte[] pixels = ((DataBufferByte) im.getRaster().getDataBuffer())
//                .getData();
//        // Create a Matrix the same size of image
//        Mat mat = new Mat(im.getHeight(), im.getWidth(), CvType.CV_8UC3);
//        // Fill Matrix with image values
//        mat.put(0, 0, pixels);
//        return mat;
//
//    }

    public HashMap<String, Float> predict(List<CalculateInfo> images)
    {
        if(images == null || images.size() == 0)
            return null;
        return predict(images, 0, null, null, 102, 0);
    }

    private native HashMap<String, Float> predict(List<CalculateInfo> images,
                                                  int reset,
                                                  String modelFile,
                                                  String trainedFile,
                                                  int batchSize,
                                                  int gpuId);


}
