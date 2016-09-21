package com.persist.bean.analysis;

import java.io.Serializable;

/**
 * Created by zhiheng on 2016/7/4.
 *
 * Picture base info
 *
 */
public class PictureKey implements Serializable{

    //picture local url or web url
    public String url;
    //the url of the video which the picture is grabbed from
    public String video_id;
    //the timestamp of the file specified by the url
    public String time_stamp;

    public boolean special = false;

    public PictureKey()
    {

    }

    public PictureKey(String url, String video_id, String time_stamp)
    {
        this.url = url;
        this.video_id = video_id;
        this.time_stamp = time_stamp;
        this.special = false;
    }

    @Override
    public boolean equals(Object obj) {
        if(this == obj)
            return true;
        if (obj == null || getClass() != obj.getClass())
            return false;
        PictureKey key = (PictureKey)obj;
        boolean r1 = (url == null) ? (key.url == null) : (url.equals(key.url));
        boolean r2 = (video_id == null) ? (key.video_id == null) : (video_id.equals(key.video_id));
        boolean r3 = (time_stamp == null) ? (key.time_stamp == null) : (time_stamp.equals(key.time_stamp));
        return r1 && r2 && r3;
    }

    @Override
    public int hashCode() {
        int h1 = url == null ? 0 :url.hashCode();
        int h2 = video_id == null ? 0 : video_id.hashCode();
        int h3 = time_stamp == null ? 0 : time_stamp.hashCode();
        int hashCode = h1;
        hashCode = hashCode*31+h2;
        hashCode = hashCode*31+h3;
        return hashCode;
    }
}
