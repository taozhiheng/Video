package com.persist;

import com.google.gson.Gson;
import com.persist.bean.analysis.PictureKey;
import com.persist.bean.grab.VideoInfo;
import com.persist.kafka.KafkaNewProducer;
import com.persist.util.helper.HDFSHelper;
import com.persist.util.helper.Logger;
import com.persist.util.tool.grab.IVideoNotifier;
import com.persist.util.tool.grab.VideoNotifierImpl;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacv.*;
import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;

/**
 * Created by taozhiheng on 16-7-19.
 * this class should be invoked as a child process
 * grab rtmp and write data to hdfs
 */
public class GrabThread extends Thread{

    private final static String TAG = "Grab";

    private HDFSHelper mHelper;
    private String mUrl;
    private String mDir;
    private FFmpegFrameGrabber mGrabber;
    private OpenCVFrameConverter.ToIplImage mIlplImageConverter;
    private Java2DFrameConverter mImageConverter;

    private String mFormat = "picture-%05d-%d.png";
    private int mWidth = 480;

    private boolean mIsRunning;
    private boolean mIsActive;

    private int mCount = 0;
    private int mIndex = 0;

    private IVideoNotifier mNotifier;

    private KafkaNewProducer mProducer;
    private String mTopic;

    private Gson mGson;

    public GrabThread(String url, String dir, IVideoNotifier notifier, String topic, String brokerList)
    {
        mUrl = url;
        mDir = dir;
        mHelper = new HDFSHelper(dir);
        mGrabber = new FFmpegFrameGrabber(url);
        mIlplImageConverter = new OpenCVFrameConverter.ToIplImage();
        mImageConverter = new Java2DFrameConverter();
        mNotifier = notifier;
        mTopic = topic;
        mProducer = new KafkaNewProducer(brokerList);
        mGson = new Gson();
    }

    public FFmpegFrameGrabber getGrabber()
    {
        return mGrabber;
    }

    /**
     *  start grab in a child thread
     * */
    @Override
    public void run() {
        mNotifier.prepare();
//        startGrab();
        grab();
    }

    /**
     * execute grab
     * */
    private void grab()
    {
        mIsRunning = true;
        mIsActive = true;

        Frame frame = null;
        opencv_core.IplImage image;
        int oldW;
        int oldH;
        int h;
        BufferedImage bi;
        String fileName;
        boolean res;
        long time;
        PictureKey pictureKey = new PictureKey();
        try
        {
            mGrabber.start();
            mNotifier.notify("finish starting");
            while (mIsRunning)
            {

                if(!mIsActive)
                {
                    Logger.log(TAG, "in pause");
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    continue;
                }
                //this is a block method
//                frame = mGrabber.grabFrame(false, true, true, false);
                frame = mGrabber.grab();
                image = mIlplImageConverter.convertToIplImage(frame);
                mNotifier.notify("finish grabbing frame " + mCount);
                if(image != null)
                {
                    //resize image
                    oldW = image.width();
                    oldH = image.height();
                    h = (int) (1.0*mWidth / oldW * oldH);
                    bi = new BufferedImage(mWidth, h, BufferedImage.TYPE_3BYTE_BGR);
                    bi.getGraphics().drawImage(mImageConverter.getBufferedImage(frame), 0, 0, mWidth, h, null);
                    //store image to hdfs
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    ImageIO.write(bi, "png", baos);
                    InputStream is = new ByteArrayInputStream(baos.toByteArray());
                    time = System.currentTimeMillis();
                    fileName = String.format(mFormat, mCount, time);
                    res = mHelper.upload(is, fileName);
                    mNotifier.notify("finish writing frame:" + mCount+", "+res);

                    //send message to kafka
                    pictureKey.url = mDir+File.separator+fileName;
                    pictureKey.video_id = mUrl;
                    pictureKey.time_stamp = String.valueOf(time);
                    try {
                        mProducer.send(mTopic, mGson.toJson(pictureKey));
                    } catch (Exception e) {
                        e.printStackTrace();
                        Logger.log(TAG, "Producer send message Exception, when send:"+
                                pictureKey.url+", "+pictureKey.video_id+","+pictureKey.time_stamp);
                    }

                    mCount++;
                }
                mIndex++;
            }
            mNotifier.notify("grab total: " + mCount+"/"+mIndex);
            mNotifier.stop();
        }
        catch (FrameGrabber.Exception e)
        {
            e.printStackTrace();
            Logger.log(TAG, "Frame Exception");
        }
        catch (IOException e)
        {
            e.printStackTrace();
            Logger.log(TAG, "IO Exception");
        }
    }

    /**
     * start grabbing
     * */
    private void startGrab()
    {
        mIsRunning = true;
        mIsActive = true;
        try {
            mGrabber.start();
        } catch (FrameGrabber.Exception e) {
            e.printStackTrace();
        }
        mNotifier.notify("start grab");
    }

    /**
     * restart grabbing
     * */
    private void restartGrab()
    {
        mIsRunning = true;
        mIsActive = true;
        try {
            mGrabber.restart();
        } catch (FrameGrabber.Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * stop grabbing
     * */
    public void stopGrab()
    {
        mIsRunning = false;
        mIsActive = false;
        mNotifier.notify("stop grab");
    }

    /**
     * pause grabbing
     * */
    public void pauseGrab()
    {
        mIsActive = false;
        mNotifier.notify("pause grab");

    }

    /**
     * continue grabbing
     * */
    public void continueGrab()
    {
        mIsActive = true;
        mNotifier.notify("continue grab");
    }

    public boolean isRunning()
    {
        return mIsRunning;
    }

    public boolean isActive()
    {
        return mIsActive;
    }

    public int getCount()
    {
       return mCount;
    }

    public int getIndex()
    {
        return mIndex;
    }


    /**
     * set output format like mp4
     * It seems that the method needn't be invoked when grab rtmp stream
     * */
    public void setOutputFormat(String outputFormat)
    {
        mGrabber.setFormat(outputFormat);
    }

    /**
     * set output file name format like frame-%05d.png
     * */
    public void setNameFormat(String format)
    {
        this.mFormat = format;
    }


    /**
     * set picture width
     * */
    public void setWidth(int width)
    {
        this.mWidth = width;
    }


    /**
     * listen msg
     * */
    static class ListenThread extends Thread
    {
        private BufferedReader reader;
        private String STOP;

        private MessageListener listener;

        public interface MessageListener
        {
            void handleMessage(String msg);
        }

        public ListenThread(BufferedReader reader, String stop)
        {
            this.reader = reader;
            this.STOP = stop;
        }

        public void setListener(MessageListener l)
        {
            this.listener = l;
        }

        @Override
        public void run() {

            String msg;
            while (true)
            {
                try {
                    msg = reader.readLine();
                    Logger.log(TAG, "Receive:"+msg);
                    if(listener != null)
                        listener.handleMessage(msg);
                    if(msg == null || STOP.equals(msg))
                        break;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    /**
     * Note:
     * this method contains some test values which should be modify and rebuilt
     *
     * the main method need at lease two arguments
     * args[0] rtmp url
     * args[2] hdfs absolute directory path (including ip or hostname)
     * */
    public static void main(String[] args)
    {
        if(args.length < 7)
            throw new RuntimeException("the main method of GrabThread need at lease 7 arguments");
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        String password = args[2];
        String url = args[3];
        String dir = args[4];
        String topic = args[5];
        String brokerList = args[6];

        //this log file is just for test
        try
        {
            Logger.setOutput(new FileOutputStream("GrabThread", true));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            Logger.setDebug(false);
        }

        //this notifier is not needed
        IVideoNotifier notifier = new VideoNotifierImpl(
                host, port,
                password, new String[]{url});

        final GrabThread grabThread = new GrabThread(url, dir, notifier, topic, brokerList);
//        grabThread.setOutputFormat(format);
        if(args.length >= 8)
            grabThread.setNameFormat(args[7]);
        grabThread.start();

        //start a ListenThread
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        ListenThread listenThread = new ListenThread(reader, VideoInfo.DEL);
        listenThread.setListener(new ListenThread.MessageListener() {
            public void handleMessage(String msg) {
                if(msg.equals(VideoInfo.DEL))
                {
                    grabThread.stopGrab();
                }
                else if(msg.equals(VideoInfo.PAUSE))
                {
                    grabThread.pauseGrab();
                }
                else if(msg.equals(VideoInfo.CONTINUE))
                {
                    grabThread.continueGrab();
                }
            }
        });
        listenThread.start();

        try {
            grabThread.join();
            listenThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        finally {
            Logger.close();
        }

    }


}