package com.persist.util.tool.analysis;

import com.persist.bean.analysis.CalculateInfo;
import com.persist.bean.analysis.PictureKey;
import com.persist.bean.analysis.PictureResult;
import com.persist.util.helper.FileLogger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by taozhiheng on 16-8-11.
 *
 */
public class Predict {

    private final static String TAG = "Predict";

    private static float warnValue = 0.75f;
    private static int bufferSize = 1000;
    private static long duration = 3000;
    private static long lastTime;

    private static boolean hasLoad = false;

    private static String so = "/home/hadoop/lib/libcaffe.so";

    private static FileLogger mLogger = new FileLogger("picture-predict");

    //images info buffer
    private static Map<String, PictureKey> infoBuffer = new HashMap<String, PictureKey>();
    //images data buffer
    private static List<CalculateInfo> buffer = new ArrayList<CalculateInfo>();

    public static void init(String lib)
    {
        so = lib;
        mLogger.log(TAG, "set so="+so);
        lastTime = System.currentTimeMillis();
    }

    public static void setWarnValue(float value)
    {
        warnValue = value;
    }

    public static void setBufferSize(int size)
    {
        bufferSize = size;
    }

    public static void setDuration(long d)
    {
        duration = d;
    }

    public synchronized static boolean append(PictureKey pictureKey, CalculateInfo calculateInfo)
    {
        if(pictureKey != null && calculateInfo != null) {
            infoBuffer.put(pictureKey.url, pictureKey);
            buffer.add(calculateInfo);
            long cur = System.currentTimeMillis();
            long d = cur-lastTime;
            return buffer.size() >= bufferSize || d >= duration;
        }
        return false;
    }

    public synchronized static List<PictureResult> triggerPredict(boolean force)
    {
        long start, predictTime;
        start = System.currentTimeMillis();
        boolean sizeReady = buffer.size() >= bufferSize;
        boolean timeReady = (start-lastTime) >= duration;
        if(!force && !sizeReady && !timeReady)
            return null;
        HashMap<String, Float> map = predictProxy(buffer);
        predictTime = System.currentTimeMillis()-start;
        List<PictureResult> results = null;
        if(map != null)
        {
            //construct results
            results = new ArrayList<PictureResult>(map.size());
            float value;
            boolean ok;
            PictureKey pictureKey;
            for (Map.Entry<String, Float> entry : map.entrySet())
            {
                value = entry.getValue();
                ok = value < warnValue;
                pictureKey = infoBuffer.get(entry.getKey());
                if (pictureKey == null)
                    pictureKey = new PictureKey(entry.getKey(), "rtmp://unknown", "Unknown");
                results.add(new PictureResult(pictureKey, ok, value));
            }
        }
        buffer.clear();
        infoBuffer.clear();
        lastTime = System.currentTimeMillis();
        mLogger.log("Predict", "time="+predictTime+" ms, size="
                +(results == null ? 0 : results.size())+", buffer="+buffer.size()
                +", sizeReady="+sizeReady+", timeReady="+timeReady);
        return results;
    }

    public static HashMap<String, Float> predictProxy(List<CalculateInfo> images)
    {
        //only load library once
        if(!hasLoad)
        {
            if(so.contains(".so"))
                System.load(so);
            else
                System.loadLibrary(so);
            mLogger.log(TAG, "load library from "+so);
            hasLoad = true;
        }
        //cpp library don't check null, make the invoke safe and avoid invalid invoke (size=0)
        if(images == null || images.size() == 0)
            return null;
        return predict(images);
    }

    /**
     * real prediction algorithm which is implemented by cpp, and invoked by XXX.so in linux
     *
     * how to build XXX.so:
     * No.1:    javah com.persist.util.tool.analysis.Predict
     * No.2:    g++ -I$JAVA_HOME/include -I$JAVA_HOME/include/linux
     *          -fPIC -shared com_persist_util_tool_analysis_Predict.cpp -o libcaffe.so
     * */
    private static native HashMap<String, Float> predict(List<CalculateInfo> images);

}
