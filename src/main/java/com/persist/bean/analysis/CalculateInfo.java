package com.persist.bean.analysis;

import java.io.Serializable;

/**
 * Created by taozhiheng on 16-8-2.
 *
 * base data to analysis image
 *
 */
public class CalculateInfo implements Serializable{

    public String key;
    public byte[] value;
    public int rows;
    public int cols;

    public CalculateInfo(String key, byte[] value, int rows, int cols)
    {
        this.key = key;
        this.value = value;
        this.rows = rows;
        this.cols = cols;
    }
}
