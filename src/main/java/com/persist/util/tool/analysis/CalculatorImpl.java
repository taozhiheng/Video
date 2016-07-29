package com.persist.util.tool.analysis;

import com.persist.bean.analysis.PictureKey;
import com.persist.bean.analysis.PictureResult;
import com.persist.util.helper.HDFSHelper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by taozhiheng on 16-7-29.
 *
 */
public class CalculatorImpl implements IPictureCalculator {

    private HDFSHelper mHelper;
    private Map<PictureKey, byte[]> mBuffer;
    private int mBufferSize = 1000;
    private long mDuration = 2000;
    private long lastTime;

    public CalculatorImpl()
    {
        mHelper = new HDFSHelper(null);
        mBuffer = new HashMap<PictureKey, byte[]>();
        lastTime = System.currentTimeMillis();
    }

    public void prepare() {

    }

    public List<PictureResult> calculateImage(PictureKey key) {

        //put data to buffer
        try {
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            mHelper.download(os, key.url);
            mBuffer.put(key, os.toByteArray());
            os.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        long curTime = System.currentTimeMillis();
        if(mBuffer.size() >= mBufferSize || curTime-lastTime >= mDuration)
        {
            //calculate

            //return

            //clear
            mBuffer.clear();
        }
        lastTime = curTime;
        return null;
    }
}
