package com.persist.util.tool.grab;

import java.io.File;
import java.io.IOException;

/**
 * Created by taozhiheng on 16-7-16.
 * simple grab implementation
 * just try to start a child process by invoking cmd(executable process not lib)
 *
 * useless
 * @deprecated
 */
public class SimpleGrabberImpl implements IGrabber {

    private String ffmpeg;
    private String format;

    public SimpleGrabberImpl(String ffmpeg, String format)
    {
        this.ffmpeg = ffmpeg;
        this.format = format;
    }

    /**
     * cmd command to grab frames from rtmp and directly store it to hdfs
     * cmd -i rtsp://10.0.37.150:8554/test.mkv -f avi - | hadoop fs -put - /user/maddy/test.avi
     * */
    public Process grab(String host, int port, String password, String url, String dst, String topic, String brokerList) {
        StringBuilder builder = new StringBuilder(ffmpeg);
        builder.append(" -i \"")
                .append(url)
                .append("\" live=1 ")
//                .append(" -t 0.001 ")
                .append(" -f image2 - | hadoop fs -put - ")
                .append(dst)
                .append(File.separator)
                .append(format);

        try {
            return Runtime.getRuntime().exec (builder.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
