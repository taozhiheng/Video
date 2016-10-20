package com.persist.bean.image;

import java.io.Serializable;

/**
 * Created by taozhiheng on 16-8-4.
 *
 * the images message info
 *
 */
public class ImageInfo implements Serializable{

    public String[] urls;
    public float[] values;
    public boolean[] oks;

    //just for identification
    public String user;

    //just for authentication
    public String username;
    public String password;

    public ImageInfo()
    {

    }

    public ImageInfo(String[] urls, float[] values, boolean[] oks)
    {
        this.urls = urls;
        this.values = values;
        this.oks = oks;
    }
}
