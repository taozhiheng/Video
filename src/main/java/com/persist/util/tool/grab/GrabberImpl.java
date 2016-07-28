package com.persist.util.tool.grab;

import com.persist.util.helper.Logger;

import java.io.IOException;

/**
 * Created by taozhiheng on 16-7-15.
 * invoke java .class to start grabbing frames in a child process
 */
public class GrabberImpl implements IGrabber {

    private final static String TAG = "GrabberImpl";
    private String cmd;
    private String nameFormat;

    public GrabberImpl(String cmd)
    {
        this.cmd = cmd;
    }

    public GrabberImpl(String cmd, String format)
    {
        this.cmd = cmd;
        this.nameFormat = format;
    }

    /**
     * grab rtmp with GrabThread
     * */
    public Process grab(String host, int port, String password, String url, String dir, String sendTopic, String brokerList)
    {
        try {
            StringBuilder builder = new StringBuilder(cmd);
            builder.append(' ').append(host).append(' ').append(port).append(' ').append(password);
            builder.append(' ').append(url).append(' ').append(dir);
            builder.append(' ').append(sendTopic).append(' ').append(brokerList);
            if(nameFormat != null)
                builder.append(' ').append(nameFormat);
            Logger.log(TAG, "execute command:"+builder.toString());
            return Runtime.getRuntime().exec(builder.toString());
        } catch (IOException e) {
            e.printStackTrace();
            Logger.log(TAG, "process Exception:"+e.getMessage());
            return null;
        }
    }
}