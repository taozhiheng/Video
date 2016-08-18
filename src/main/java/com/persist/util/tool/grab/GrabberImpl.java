package com.persist.util.tool.grab;

import java.io.IOException;
import java.util.Map;

/**
 * Created by taozhiheng on 16-7-15.
 *
 * invoke java .class (GrabThread.class) to start grabbing frames in a child process
 *
 */
public class GrabberImpl implements IGrabber {

    private final static String TAG = "GrabberImpl";
    private String cmd;
    private double frameRate = 1.0;
    private String nameFormat;

    private final static String STORM_HOME = "STORM_HOME";

    public GrabberImpl(String cmd)
    {
        this.cmd = cmd;
    }

    public GrabberImpl(String cmd, String format)
    {
        this.cmd = cmd;
        this.nameFormat = format;
    }

    public GrabberImpl(String cmd, String format, double rate)
    {
        this.cmd = cmd;
        this.nameFormat = format;
        this.frameRate = rate;
    }

    public void setFrameRate(double rate)
    {
        if(rate > 0)
            this.frameRate = rate;
    }

    /**
     * grab rtmp with GrabThread
     * */
    public Process grab(String host, int port, String password, String url, String dir, String sendTopic, String brokerList)
    {
        try {

            Map<String, String> map = System.getenv();
            String value = map.get(STORM_HOME);

            StringBuilder builder = new StringBuilder(cmd);
            builder.append(' ').append(host).append(' ').append(port).append(' ').append(password);
            builder.append(' ').append(url).append(' ').append(dir);
            builder.append(' ').append(sendTopic).append(' ').append(brokerList);
            builder.append(' ').append(frameRate);
            if(nameFormat != null)
                builder.append(' ').append(nameFormat);
            String cmd = builder.toString().replace("$"+STORM_HOME, value);

            return Runtime.getRuntime().exec(cmd, new String[]{STORM_HOME+"="+value});
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
