package com.persist.bean.grab;

import java.io.Serializable;

/**
 * Created by taozhiheng on 16-7-15.
 *
 * video message info
 *
 */
public class VideoInfo implements Serializable{

    //the url path of the video
    public String url;
    //the dst path to store pictures
    public String dir;
    //the operation type;
    public String cmd;

    public final static String ADD = "add";
    public final static String PAUSE = "stop";
    public final static String CONTINUE = "start";
    public final static String DEL = "quit";

    public VideoInfo()
    {

    }

    public VideoInfo(String url, String dir, String cmd)
    {
        this.url = url;
        this.dir = dir;
        this.cmd = cmd;
    }
}
