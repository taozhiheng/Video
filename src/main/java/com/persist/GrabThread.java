package com.persist;

import com.google.gson.Gson;
import com.persist.bean.analysis.PictureKey;
import com.persist.bean.grab.VideoInfo;
import com.persist.kafka.KafkaNewProducer;
import com.persist.util.helper.BufferedImageHelper;
import com.persist.util.helper.FileLogger;
import com.persist.util.helper.HDFSHelper;
import com.persist.util.helper.ImageHelper;
import com.persist.util.tool.grab.IVideoNotifier;
import com.persist.util.tool.grab.VideoNotifierImpl;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.InvalidTimestampException;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacv.*;
import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;
import java.lang.management.ManagementFactory;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by taozhiheng on 16-7-19.
 *
 * this class should be invoked as a child process
 *
 * grab rtmp and write data to hdfs
 *
 */
public class GrabThread extends Thread{

    private final static String TAG = "Grab";

    private final static String START_TIMEOUT_MSG = "start-timeout";
    private final static String GRAB_TIMEOUT_MSG = "grabImage-timeout";
    private final static String RESTART_TIMEOUT_MSG = "restart-timeout";

//    private final static int FAIL_SECONDS = 5;
//
//    private final static long START_TIMEOUT = 8000;
//    private final static long GRAB_TIMEOUT = 3000;
//    private final static long RESTART_TIMEOUT = 8000;

    private int mFailSeconds = 5;
    private long mStartTimeout = 8000;
    private long mGrabTimeout = 3000;
    private long mRestartTimeout = 8000;

    private HDFSHelper mHelper;
    private String mUrl;
    private String mDir;
    private FFmpegFrameGrabber mGrabber;
    private OpenCVFrameConverter.ToIplImage mIlplImageConverter;
    private Java2DFrameConverter mImageConverter;

    private String mFormat = "picture-%05d-%d.png";
    private int mWidth = 227;
    private int mHeight = 227;
    //grab rate
    private double mGrabRate = 1.0;

    private boolean mIsRunning;
    private boolean mIsActive;

    private int mCount = 0;
    private int mIndex = 0;

    private IVideoNotifier mNotifier;

    private KafkaNewProducer mProducer;
    private Callback mCallback;

    private String mTopic;

    private Gson mGson;

    private FileLogger mLogger;

    private int retry = 0;

    private int failLimit = 10;
    private int sameLimit = 5;

    private MessageListener mListener;

    private ScheduledExecutorService mService;
    private ScheduledFuture startFuture;
    private ScheduledFuture grabFuture;
    private ScheduledFuture restartFuture;
    //timeout flag
    boolean[] flags = new boolean[3];
    //timeout task
    private Runnable startRunnable = new Runnable() {
        public void run() {
            if(flags[0] && mListener != null)
                mListener.handleMessage(GrabThread.this, START_TIMEOUT_MSG);
        }
    };
    private Runnable grabRunnable = new Runnable() {
        public void run() {
            if(flags[1] && mListener != null)
                mListener.handleMessage(GrabThread.this, GRAB_TIMEOUT_MSG);
        }
    };
    private Runnable restartRunnable = new Runnable() {
        public void run() {
            if(flags[2] && mListener != null)
                mListener.handleMessage(GrabThread.this, RESTART_TIMEOUT_MSG);
        }
    };

    public GrabThread(String url, String dir, IVideoNotifier notifier, String topic, String brokerList, FileLogger logger)
    {
        mUrl = url;
        mDir = dir;
        String id = String.valueOf(Math.abs(url.hashCode()));
        mFormat = id +"-%05d-%d.png";
        mLogger = logger;
        mHelper = new HDFSHelper(dir);
        mGrabber = new FFmpegFrameGrabber(url);
        mIlplImageConverter = new OpenCVFrameConverter.ToIplImage();
        mImageConverter = new Java2DFrameConverter();
        mNotifier = notifier;
        mTopic = topic;
        //brokerList: ip:port,ip:port...
        if(brokerList != null && brokerList.length() > 2) {
            mProducer = new KafkaNewProducer(brokerList, false);
            mCallback = new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e != null) {
                        mLogger.log(mUrl, "Kafka Exception:");
                        e.printStackTrace(mLogger.getPrintWriter());
                        mLogger.getPrintWriter().flush();
                        mLogger.log(mUrl, "The offset of the record is: "+recordMetadata.offset());
                    }
                    else
                    {
                        mLogger.log(mUrl, "The offset of the record is: "+recordMetadata.offset());
                    }
                }
            };
        }
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
        opencv_core.IplImage image = null;
        int oldW;
        int oldH;
        BufferedImage bi = null;
        ByteArrayOutputStream baos = null;
//        OutputStream baos = null;
        String fileName = null;
        boolean res = false;
        long time;
        int grabStep = 1;
        int lastNumber = -1;
        int frameNumber = 0;

        int failTimes = 0;
        int sameTimes = 0;
        boolean grabOK = true;
        boolean grabNew = true;

        int num = 0;

        PictureKey pictureKey = new PictureKey();
        try
        {
            mNotifier.notify("prepare to start grabbing video from "+mUrl);
            mLogger.log(mUrl, "prepare to start grabbing");
            //add timeout check
            if(mService != null)
            {
                flags[0] = true;
                startFuture = mService.schedule(startRunnable, mStartTimeout, TimeUnit.MILLISECONDS );
            }
            mGrabber.start();
            flags[0] = false;
            if(startFuture != null)
            {
                startFuture.cancel(true);
            }
            double frameRate = mGrabber.getFrameRate();
            failLimit = (int)(frameRate*mFailSeconds);
            sameLimit = (int)(mGrabRate*mFailSeconds);
            grabStep = (int) (frameRate/mGrabRate);
            mNotifier.notify("finish starting, " +
                    "frameRate="+frameRate
                    +", frameLength=" + mGrabber.getLengthInFrames()
                    +", grabStep="+grabStep);
            mLogger.log(mUrl, "finish starting, " +
                    "frameRate="+frameRate
                    +", frameLength="+mGrabber.getLengthInFrames()
                    +", grabStep="+grabStep);
            //start grabbing frames
            while (mIsRunning)
            {
                //check pause signal
                if(!mIsActive)
                {
                    mLogger.log(mUrl, "in pause");
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace(mLogger.getPrintWriter());
                        mLogger.getPrintWriter().flush();
                    }
                    continue;
                }
                //grab a valid frame
                mNotifier.notify("prepare to grab frame " + mCount+"/"+mIndex);
                mLogger.log(mUrl, "prepare to grab frame " + mCount+"/"+mIndex);
                try {
                    if(mService != null)
                    {
                        mLogger.log(mUrl, "add timeout task grabImage");
                        flags[1] = true;
                        grabFuture = mService.schedule(grabRunnable, mGrabTimeout, TimeUnit.MILLISECONDS);
                    }
                    frame = mGrabber.grabImage();
                    flags[1] = false;
                    if(grabFuture != null)
                    {
                        mLogger.log(mUrl, "cancel timeout task grabImage");
                        grabFuture.cancel(true);
                    }
                    if(frame != null)
                    {
                        num++;
                        grabOK = true;
                        failTimes = 0;
                    }
                    else
                    {
                        mLogger.log(mUrl, "the frame is empty");
                        mNotifier.notify("the frame is empty");
                        if(!grabOK)
                        {
                            failTimes++;
                            if (failTimes >= failLimit && restartGrab())
                            {
                                failTimes = 0;
                                mLogger.log(mUrl, "restart grab " + mUrl+" because "+failLimit+" empty frames");
                                mNotifier.notify("restart grab " + mUrl+" because "+failLimit+" empty frames");
                            }
                        }
                        grabOK = false;
                        continue;
                    }
                    //grab a frame each group(N=num frames)
                    if(num < grabStep)
                        continue;
                    else
                        num = 0;

                    frameNumber = mGrabber.getFrameNumber();
                    mLogger.log(TAG, "frame number="+frameNumber);
                    //grab the same frame, continue
                    if(frameNumber == lastNumber)
                    {
                        mLogger.log(TAG, "grab same frame: "+frame);
                        if(!grabNew)
                        {
                            sameTimes++;
                            if (sameTimes >= sameLimit && restartGrab()) {
                                sameTimes = 0;
                                mLogger.log(mUrl, "restart grab " + mUrl+" because "+sameLimit+" same frames");
                                mNotifier.notify("restart grab " + mUrl+" because "+sameLimit+" same frames");
                            }
                        }
                        grabNew = false;
                        continue;
                    }
                    //grab different frame, record the frame number
                    else
                    {
                        lastNumber = frameNumber;
                        grabNew = true;
                        sameTimes = 0;
                    }
                } catch (FrameGrabber.Exception e) {
                    e.printStackTrace(mLogger.getPrintWriter());
                    mLogger.getPrintWriter().flush();
                    continue;
                }

                //write frame
                mNotifier.notify("grab frame " + mCount+"/"+mIndex);
                mLogger.log(mUrl, "grab frame " + mCount+"/"+mIndex);
                image = mIlplImageConverter.convertToIplImage(frame);
                if(image != null)
                {
                    //resize image
                    oldW = image.width();
                    oldH = image.height();
                    bi = new BufferedImage(oldW, oldH, BufferedImage.TYPE_3BYTE_BGR);
                    bi.getGraphics().drawImage(mImageConverter.getBufferedImage(frame), 0, 0, oldW, oldH, null);
//                    bi = ImageHelper.resize(bi, mWidth, mHeight);
                    bi = BufferedImageHelper.resize(bi, mWidth, mHeight);

                    mLogger.log(mUrl, "size: "+bi.getWidth()+"*"+bi.getHeight());
                    //write image to byte array output stream
                    baos = new ByteArrayOutputStream();
                    try {
//                        baos = new FileOutputStream(mDir+File.separator+String.format(mFormat, mCount, System.currentTimeMillis()));
                        ImageIO.write(bi, "png", baos);
                    } catch (IOException e) {
                        e.printStackTrace(mLogger.getPrintWriter());
                        mLogger.getPrintWriter().flush();
                    }
                    //write data in byte array output stream to hdfs
                    InputStream is = new ByteArrayInputStream(baos.toByteArray());
                    time = System.currentTimeMillis();
                    fileName = String.format(mFormat, mCount, time);
                    res = mHelper.upload(is, fileName);
                    try {
                        baos.close();
                        baos = null;
                    } catch (IOException e) {
                        e.printStackTrace(mLogger.getPrintWriter());
                        mLogger.getPrintWriter().flush();
                    }
                    mNotifier.notify("write frame " + mCount+" to "+fileName+", "+res);
                    mLogger.log(mUrl, "write frame " + mCount+" to "+fileName+", "+res);
                    //send message to kafka
                    pictureKey.url = mDir+File.separator+fileName;
                    pictureKey.video_id = mUrl;
                    pictureKey.time_stamp = String.valueOf(time);
                    if(mProducer != null)
                    {
                        String msg = mGson.toJson(pictureKey);
                        //Note:
                        //the process may be blocked even dead when kafka Error happened such as:
                        //org.apache.kafka.common.errors.InvalidTimestampException:
                        //The timestamp of the message is out of acceptable range.
                        //(The Exception will be displayed in mCallback, I know neither the reason nor how to fix it!!!)

                        try {
                            mProducer.send(mTopic, msg, mCallback);
                            mLogger.log(mTopic, "send kafka msg:" + msg);
                        }
                        catch (InvalidTimestampException e)
                        {
                            mLogger.log(mUrl, "send kafka fail");
                            e.printStackTrace(mLogger.getPrintWriter());
                            mLogger.getPrintWriter().flush();
                        }
                    }
                    mCount++;
                }
                else
                {
                    mLogger.log(mUrl, "sorry, the image of frame "+mIndex+" is null");
                }
                mIndex++;
            }
            mNotifier.notify("grabbing total: " + mCount+"/"+mIndex+" in "+mUrl);
            mLogger.log(mUrl, "grabbing total: " + mCount+"/"+mIndex);
            mGrabber.stop();
            mGrabber.release();

        }
        catch (FrameGrabber.Exception e)
        {
            e.printStackTrace(mLogger.getPrintWriter());
            mLogger.getPrintWriter().flush();
        }
        catch (Exception e)
        {
            e.printStackTrace(mLogger.getPrintWriter());
            mLogger.getPrintWriter().flush();
        }
        finally {
            clear();
        }
    }

    /**
     * release resources
     * */
    private void clear()
    {
        cancelAllTimeout();

        if(mHelper != null)
        {
            mHelper.close();
            mHelper = null;
        }

        if(mProducer != null)
        {
            mProducer.close();
            mProducer = null;
        }

//        if(mNotifier != null)
//        {
//            mNotifier.stop();
//            mNotifier = null;
//        }
//
//        if(mLogger != null)
//        {
//            mLogger.close();
//            mLogger = null;
//        }
    }

    public void cancelAllTimeout()
    {
        if(startFuture != null)
        {
            startFuture.cancel(true);
        }
        if(grabFuture != null)
        {
            grabFuture.cancel(true);
        }
        if(restartFuture != null)
        {
            restartFuture.cancel(true);
        }
    }

    /**
     * set thread index
     * */
    public void setRetry(int retry)
    {
        this.retry = retry;
    }

    public int retry()
    {
        retry++;
        return retry;
    }

    public void setFailSeconds(int s)
    {
        if(s > 0)
            mFailSeconds = s;
    }

    public void setStartTimeout(long timeout)
    {
        if(timeout > 0)
            mStartTimeout = timeout;
    }

    public void setGrabTimeout(long timeout)
    {
        if(timeout > 0)
            mGrabTimeout = timeout;
    }

    public void setRestartTimeout(long timeout)
    {
        if(timeout > 0)
            mRestartTimeout = timeout;
    }

    /**
     * start grabbing
     * */
    private boolean startGrab()
    {
        mIsRunning = true;
        mIsActive = true;
        try {
            mGrabber.start();
            mNotifier.notify("start grabbing");
            mLogger.log(mUrl, "start grabbing");
            return true;
        } catch (FrameGrabber.Exception e) {
            e.printStackTrace(mLogger.getPrintWriter());
            mLogger.getPrintWriter().flush();
            return false;
        }

    }

    /**
     * restart grabbing
     * @see #grab() has contains this, so no need to invoke it
     * */
    private boolean restartGrab()
    {
        mIsRunning = true;
        mIsActive = true;
        try {
            if(mService != null)
            {
                flags[2] = true;
                restartFuture = mService.schedule(restartRunnable, mRestartTimeout, TimeUnit.MILLISECONDS);
            }
            mNotifier.notify("stop grabbing");
            mLogger.log(mUrl, "stop grabbing");
            mGrabber.restart();
            flags[2] = false;
            if(restartFuture != null)
            {
                restartFuture.cancel(true);
            }
            return true;
        } catch (FrameGrabber.Exception e) {
            e.printStackTrace(mLogger.getPrintWriter());
            mLogger.getPrintWriter().flush();
            return false;
        }
    }

    /**
     * stop grabbing
     * */
    public void stopGrab()
    {
        mIsRunning = false;
        mIsActive = false;
        mNotifier.notify("stop grabbing");
        mLogger.log(mUrl, "stop grabbing");
    }

    /**
     * pause grabbing
     * */
    public void pauseGrab()
    {
        mIsActive = false;
        mNotifier.notify("pause grabbing");
        mLogger.log(mUrl, "pause grabbing");
    }

    /**
     * continue grabbing
     * */
    public void continueGrab()
    {
        mIsActive = true;
        mNotifier.notify("continue grabbing");
        mLogger.log(mUrl, "continue grabbing");
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

    public double getGrabRate()
    {
        return mGrabRate;
    }

    /**
     * set grab rate, how many frames to be grab each second
     * default value: 1f
     * */
    public void setGrabRate(double rate)
    {
        if(rate > 0)
            mGrabRate = rate;
    }

    /**
     * set output format like mp4
     * It seems that the method needn't be invoked
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
     * set picture size
     * */
    public void setSize(int width, int height)
    {
        this.mWidth = width;
        this.mHeight = height;
    }

    /**
     * set schedule executor service
     * */
    public void setService(ScheduledExecutorService service)
    {
        mService = service;
    }

    public void setListener(MessageListener l)
    {
        this.mListener = l;
    }

    interface MessageListener
    {
        void handleMessage(Thread thread, String msg);
    }

    /**
     * listen message
     * the thread should run only if the process is a child process,
     * otherwise there will be some IOExceptions.
     * */
    static class ListenThread extends Thread
    {
        private BufferedReader reader;
        private String STOP;
        private boolean run;

        private GrabThread grabThread;

        private MessageListener listener;

        public ListenThread(BufferedReader reader, String stop)
        {
            this.reader = reader;
            this.STOP = stop;
            this.run = true;
        }

        public void setGrabThread(GrabThread t)
        {
            grabThread = t;
        }

        public void setListener(MessageListener l)
        {
            this.listener = l;
        }

        @Override
        public void run() {

            String msg;
            while (run)
            {
                try {
                    msg = reader.readLine();
                    if(listener != null)
                        listener.handleMessage(grabThread, msg);
                    if(msg == null || STOP.equals(msg))
                        break;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        public void stopListen()
        {
            this.run = false;
        }
    }

    /**
     * Note:
     * this method contains some test values which should be modify and rebuilt
     *
     * the main method need at lease 7 arguments
     * args[0] redis host
     * args[1] redis port
     * args[2] redis password
     * args[3] video rtmp url
     * args[4] hdfs absolute directory path (including ip or hostname)
     * args[5] kafka topic to send message
     * args[6] kafka brokerList to send message
     * args[7] file name format [optional]
     * */

    static GrabThread createGrabThread(String url, String dir, IVideoNotifier notifier,
                                       String topic, String brokerList, FileLogger logger,
                                       double rate, int failSeconds, long startTimeout, long grabTimeout,
                                       long restartTimeout, MessageListener listener, ScheduledExecutorService service)
    {
        GrabThread grabThread = new GrabThread(url, dir, notifier, topic, brokerList, logger);
        grabThread.setGrabRate(rate);
        grabThread.setFailSeconds(failSeconds);
        grabThread.setStartTimeout(startTimeout);
        grabThread.setGrabTimeout(grabTimeout);
        grabThread.setRestartTimeout(restartTimeout);
        grabThread.setListener(listener);
        grabThread.setService(service);
        return grabThread;
    }

    public static void main(String[] args)
    {

        if(args.length < 7)
            throw new RuntimeException("the main method of GrabThread need at lease 7 arguments");
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        String password = args[2];
        final String url = args[3];
        final String dir = args[4];
        final String topic = args[5];
        final String brokerList = args[6];


        String name = ManagementFactory.getRuntimeMXBean().getName();
        final String pid = name.split("@")[0];
        final FileLogger logger = new FileLogger("GrabThread@"+pid);

        double rate = 1.0;
        if(args.length >= 8)
        {
            try
            {
                rate = Double.valueOf(args[7]);
            }
            catch (NumberFormatException e)
            {
                e.printStackTrace(logger.getPrintWriter());
                logger.getPrintWriter().flush();
            }
        }
        final double grabRate = rate;
        int seconds = 5;
        if(args.length >= 9)
        {
            try
            {
                seconds = Integer.valueOf(args[8]);
            }
            catch (NumberFormatException e)
            {
                e.printStackTrace(logger.getPrintWriter());
                logger.getPrintWriter().flush();
            }
        }
        final int failSeconds = seconds;
        long timeout1 = 8000;
        if(args.length >= 10)
        {
            try
            {
                timeout1 = Long.valueOf(args[9]);
            }
            catch (NumberFormatException e)
            {
                e.printStackTrace(logger.getPrintWriter());
                logger.getPrintWriter().flush();
            }
        }
        final long startTimeout = timeout1;
        long timeout2 = 3000;
        if(args.length >= 11)
        {
            try
            {
                timeout2 = Long.valueOf(args[10]);
            }
            catch (NumberFormatException e)
            {
                e.printStackTrace(logger.getPrintWriter());
                logger.getPrintWriter().flush();
            }
        }
        final long grabTimeout = timeout2;
        long timeout3 = 8000;
        if(args.length >= 12)
        {
            try
            {
                timeout3 = Long.valueOf(args[11]);
            }
            catch (NumberFormatException e)
            {
                e.printStackTrace(logger.getPrintWriter());
                logger.getPrintWriter().flush();
            }
        }
        final long restartTimeout = timeout3;
        int times = 3;
        if(args.length >= 13)
        {
            try
            {
                times = Integer.valueOf(args[12]);
            }
            catch (NumberFormatException e)
            {
                e.printStackTrace(logger.getPrintWriter());
                logger.getPrintWriter().flush();
            }
        }
        final int retry = times;


        //this notifier is not needed
        final IVideoNotifier notifier = new VideoNotifierImpl(
                host, port,
                password, new String[]{url});


        //construct  listen thread
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        final ListenThread listenThread = new ListenThread(reader, VideoInfo.DEL);

        final GrabThread[] threads = new GrabThread[retry];
        //construct grabThread
        final ScheduledExecutorService service = new ScheduledThreadPoolExecutor(5);
        final MessageListener listener = new MessageListener()
        {
            public void handleMessage(Thread thread, String msg)
            {
                if (thread instanceof GrabThread)
                {
                    GrabThread t = (GrabThread) thread;
                    //send message to report
                    if (START_TIMEOUT_MSG.equals(msg)) {
                        logger.log(url, "Start timeout "+t.mStartTimeout+".You'd better destroy the task, then start a new task.");
                        notifier.notify("Start timeout "+t.mStartTimeout+".You'd better destroy the task, then start a new task.");
                    } else if (GRAB_TIMEOUT_MSG.equals(msg)) {
                        logger.log(url, "Grab timeout "+t.mGrabTimeout+".If it happened for many times, You'd better destroy the task, then start a new task.");
                        notifier.notify("Grab timeout "+t.mGrabTimeout+".If it happened for many times, You'd better destroy the task, then start a new task.");
                    } else if (RESTART_TIMEOUT_MSG.equals(msg)) {
                        logger.log(url, msg + "Restart timeout "+t.mRestartTimeout+".You'd better destroy the task, then start a new task.");
                        notifier.notify(msg + "Restart timeout "+t.mRestartTimeout+".You'd better destroy the task, then start a new task.");
                    } else {
                        logger.log(url, "Unknown timeout");
                        notifier.notify("Unknown timeout");
                    }
                    //can retry
                    int index = t.retry();
                    if (index < threads.length)
                    {
                        t.cancelAllTimeout();
                        t.interrupt();
                        if (index - 1 >= 0)
                            threads[index - 1] = null;
                        logger.log(url, "Retry");
                        notifier.notify("Retry");
                        threads[index] = createGrabThread(
                                url, dir, notifier,
                                topic, brokerList, logger,
                                grabRate, failSeconds, startTimeout,
                                grabTimeout, restartTimeout, this, service);
                        threads[index].setRetry(index);
                        threads[index].start();
                        //set listen thread
                        listenThread.setGrabThread(threads[index]);
                    } else
                    {
                        logger.log(url, "The grab task will be destroyed after "+retry+" retries, you can start a new task after repairing problems.");
                        notifier.notify("The grab task will be destroyed after "+retry+" retries, you can start a new task after repairing problems.");
                        notifier.stop();
                        listenThread.stopListen();
                        logger.log(url, "Child process: GrabThread  " + pid + " has exit");
                        logger.close();
                        System.exit(0);
                    }
                }
            }
        };
        threads[0] = createGrabThread(
                url, dir, notifier,
                topic, brokerList, logger,
                grabRate, failSeconds, startTimeout,
                grabTimeout, restartTimeout, listener, service);
        threads[0].setRetry(0);
        //start grab thread
        threads[0].start();
        //set listen thread
        listenThread.setGrabThread(threads[0]);
        listenThread.setListener(new MessageListener() {
            public void handleMessage(Thread thread, String msg) {
                if(thread != null && thread instanceof GrabThread)
                {
                    GrabThread t = (GrabThread)thread;
                    if (msg.equals(VideoInfo.DEL)) {
                        t.stopGrab();
                    } else if (msg.equals(VideoInfo.PAUSE)) {
                        t.pauseGrab();
                    } else if (msg.equals(VideoInfo.CONTINUE)) {
                        t.continueGrab();
                    }
                }
            }
        });
        //start listen thread
        if(brokerList != null && brokerList.length() > 2)
            listenThread.start();

        logger.log(url, "Child process: GrabThread is running in "+pid);
        try
        {
            if(threads[0] != null)
                threads[0].join();
            if(threads[1] != null)
                threads[1].join();
            if(threads[2] != null)
                threads[3].join();
        } catch (InterruptedException e) {
            e.printStackTrace(logger.getPrintWriter());
            logger.getPrintWriter().flush();
        }
        finally {
            listenThread.stopListen();
            notifier.notify("Child process: GrabThread  " + pid + " has exit");
            notifier.stop();
            logger.log(url, "Child process: GrabThread  " + pid + " has exit");
            logger.close();
            System.exit(0);
        }
    }


}
