package com.persist.bean.analysis;

import java.io.Serializable;

/**
 * Created by taozhiheng on 16-8-4.
 *
 */
public class ImageInfo implements Serializable{

    public String url;
    public float value = -1;
    public boolean ok;

    public ImageInfo()
    {

    }

    public ImageInfo(String url, float value, boolean ok)
    {
        this.url = url;
        this.value = value;
        this.ok = ok;
    }
}
