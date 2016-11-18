package com.persist.util.tool.grab;

import com.persist.util.helper.FileLogger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
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
    private int failSeconds = 5;
    private long startTimeout = 8000;
    private long grabTimeout = 3000;
    private long restartTimeout = 8000;
    private int retry = 3;

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

    public GrabberImpl(String cmd, String format, double rate,
                       int failSeconds, long startTimeout, long grabTimeout,
                       long restartTimeout, int retry)
    {
        this.cmd = cmd;
        this.nameFormat = format;
        this.frameRate = rate;
        this.failSeconds = failSeconds;
        this.startTimeout = startTimeout;
        this.grabTimeout = grabTimeout;
        this.restartTimeout = restartTimeout;
        this.retry = retry;
    }

    public void setFrameRate(double rate)
    {
        if(rate > 0)
            this.frameRate = rate;
    }

    public void setFailSeconds(int s)
    {
        if(s > 0)
            failSeconds = s;
    }

    public void setStartTimeout(long timeout)
    {
        if(timeout > 0)
            startTimeout = timeout;
    }

    public void setGrabTimeout(long timeout)
    {
        if(timeout > 0)
            grabTimeout = timeout;
    }

    public void setRestartTimeout(long timeout)
    {
        if(timeout > 0)
            restartTimeout = timeout;
    }

    public void setRetry(int retry)
    {
        if(retry > 0)
            this.retry  = retry;
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
            builder.append(' ').append(failSeconds);
            builder.append(' ').append(startTimeout).append(' ').append(grabTimeout).append(' ').append(restartTimeout);
            builder.append(' ').append(retry);
            if(nameFormat != null)
                builder.append(' ').append(nameFormat);
            String cmd = builder.toString().replace("$"+STORM_HOME, value);

            Process process = Runtime.getRuntime().exec(cmd, new String[]{STORM_HOME+"="+value});
            String id = String.valueOf(Math.abs(url.hashCode()));
            new StreamThread(process.getInputStream(), id+".out", "OUT").start();
            new StreamThread(process.getErrorStream(), id+".err", "ERR").start();
            return process;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    //just for test
    static class StreamThread extends Thread
    {
        private InputStream is;
        private String filename;
//        private FileLogger fileLogger;
        private String type;

        public StreamThread(InputStream is, String filename, String type)
        {
            this.is = is;
            this.filename = filename;
            this.type = type;
//            this.fileLogger = new FileLogger(filename);
        }

        @Override
        public void run() {
            try {
                InputStreamReader isr = new InputStreamReader(is);
                BufferedReader br = new BufferedReader(isr);
                String line = null;
                while ((line = br.readLine()) != null) {
//                    fileLogger.log(type, line);
                    System.out.println(type+" > "+line);
                }
//                fileLogger.log(type, "process exits?);
                System.out.println(type+" > "+" process exits?");
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }
        }
    }
}
