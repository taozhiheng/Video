package com.persist.bean.image;

/**
 * Created by taozhiheng on 16-8-4.
 *
 */
public class CheckConfig {

    public String function = "image-check";
    public String so = "CalculatorImpl";
    public int drpcSpoutParallel = 3;
    public int imageBoltParallel = 3;
    public int returnBoltParallel = 3;
    //the num to workers
    public int workerNum= 3;

    public CheckConfig()
    {

    }

}
