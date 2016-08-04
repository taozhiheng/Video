package com.persist;

import org.opencv.core.Mat;

import java.util.Map;

/**
 * Created by taozhiheng on 16-8-2.
 *
 */
public class Sample {

    public native Map<String, Float> predict(Map<String, Mat> images);
}
