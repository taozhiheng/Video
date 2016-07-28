package com.persist.bean.analysis;

import java.io.Serializable;

/**
 * Created by zhiheng on 2016/7/4.
 * Picture base info
 */
public class PictureKey implements Serializable{

    //picture local url or web url
    public String url;
    //the url of the video which the picture is grabbed from
    public String video_id;
    //the timestamp of the file specified by the url
    public String time_stamp;

    public PictureKey()
    {

    }

    public PictureKey(String url, String video_id, String time_stamp)
    {
        this.url = url;
        this.video_id = video_id;
        this.time_stamp = time_stamp;
    }
}
