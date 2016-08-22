package com.persist;

import com.google.gson.Gson;
import com.persist.bean.analysis.PictureKey;
import com.persist.bean.grab.VideoInfo;
import com.persist.kafka.KafkaNewProducer;
import com.persist.util.helper.FileLogger;
import com.persist.util.helper.HDFSHelper;
import com.persist.util.helper.ImageHepler;
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
    private final static int FAIL_LIMIT = 20;

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
        int expectNumber = 0;
        int grabStep = 1;
        int lastNumber = -1;
        int frameNumber = 0;
        boolean setOK = true;
        int realStep;
        int errorTimes = 0;
        boolean isFirst = true;

        int num = 0;

        PictureKey pictureKey = new PictureKey();
        try
        {
            mNotifier.notify("prepare to start grabbing video from"+mUrl);
            mLogger.log(mUrl, "prepare to start grabbing");
            mGrabber.setTimeout(3000);
            mGrabber.start();
            double frameRate = mGrabber.getFrameRate();
            grabStep = (int) (frameRate/mGrabRate);
            mNotifier.notify("finish starting, " +
                    "frameRate="+frameRate
                    +", frameLength=" + mGrabber.getLengthInFrames()
                    +", grabStep="+grabStep);
            mLogger.log(mUrl, "finish starting, " +
                    "frameRate="+frameRate
                    +", frameLength="+mGrabber.getLengthInFrames()
                    +", grabStep="+grabStep);
            while (mIsRunning)
            {

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
                mNotifier.notify("prepare to grab frame " + mCount+"/"+mIndex);
                mLogger.log(mUrl, "prepare to grab frame " + mCount+"/"+mIndex);
                //set frame number
                //if set frame number ok last time, append grabStep, else append 1
//                if(grabStep > 1 && !isFirst)
//                {
//                    if(setOK)
//                        realStep = grabStep;
//                    else
//                        realStep = 1;
//                    expectNumber += realStep;
//                    try {
//                        mGrabber.setFrameNumber(expectNumber);
//                        setOK = true;
//                        errorTimes = 0;
//                        mLogger.log(mUrl, "expectNumber="+expectNumber);
//                    } catch (FrameGrabber.Exception e) {
//                        e.printStackTrace(mLogger.getPrintWriter());
//                        mLogger.getPrintWriter().flush();
//                        //reset expect number, and tag setOk=false
//                        expectNumber -= realStep;
//                        if(!setOK)
//                        {
//                            errorTimes++;
//                            //fail too many time, restart grab
//                            if(errorTimes >= FAIL_LIMIT && restartGrab()) {
//                                errorTimes = 0;
//                                isFirst = true;
//                                expectNumber = 0;
//                                mLogger.log(mUrl, "restart grab "+mUrl);
//                            }
//                        }
//                        setOK = false;
//                        continue;
//                    }
//                }
                //grab image
                try {
                    frame = mGrabber.grabImage();
//                    if(frame != null && isFirst)
//                        isFirst = false;
                    if(frame != null)
                        num++;
                    //grab a frame each num frames
                    if(num < grabStep)
                        continue;
                    else
                        num = 0;
                    frameNumber = mGrabber.getFrameNumber();
                    mLogger.log(TAG, "frame number="+frameNumber);
                    //grab the same frame, continue
                    if(frameNumber == lastNumber) {
                        mLogger.log(TAG, "grab same frame: "+frame);
                        continue;
                    }
                    //grab different frame, record the frame number
                    else
                    {
                        lastNumber = frameNumber;
                    }
                } catch (FrameGrabber.Exception e) {
                    e.printStackTrace(mLogger.getPrintWriter());
                    mLogger.getPrintWriter().flush();
                    continue;
                }

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
                    bi = ImageHepler.resize(bi, mWidth, mHeight);
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
        finally {
            clear();
        }
    }

    /**
     * release resources
     * */
    private void clear()
    {
        if(mNotifier != null)
        {
            mNotifier.stop();
            mNotifier = null;
        }

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

        if(mLogger != null)
        {
            mLogger.close();
            mLogger = null;
        }
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
            mGrabber.restart();
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
     * listen message
     * the thread should run only if the process is a child process,
     * otherwise there will be some IOExceptions.
     * */
    static class ListenThread extends Thread
    {
        private BufferedReader reader;
        private String STOP;

        private boolean run;

        private MessageListener listener;

        public interface MessageListener
        {
            void handleMessage(String msg);
        }

        public ListenThread(BufferedReader reader, String stop)
        {
            this.reader = reader;
            this.STOP = stop;
            this.run = true;
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
                        listener.handleMessage(msg);
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

        String name = ManagementFactory.getRuntimeMXBean().getName();
        String pid = name.split("@")[0];

        FileLogger logger = new FileLogger("GrabThread@"+pid);

        //this notifier is not needed
        IVideoNotifier notifier = new VideoNotifierImpl(
                host, port,
                password, new String[]{url});

        final GrabThread grabThread = new GrabThread(url, dir, notifier, topic, brokerList, logger);

        if(args.length >= 8)
        {
            try
            {
                double rate = Double.valueOf(args[7]);
                grabThread.setGrabRate(rate);
            }
            catch (NumberFormatException e)
            {
                e.printStackTrace(logger.getPrintWriter());
                logger.getPrintWriter().flush();
            }
        }
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
        if(brokerList != null && brokerList.length() > 2)
            listenThread.start();

        logger.log(url, "Child process: GrabThread is running in "+pid);

        try {
            grabThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace(logger.getPrintWriter());
            logger.getPrintWriter().flush();
        }
        finally {
            listenThread.stopListen();
            logger.log(url, "Child process: GrabThread  " + pid + " has exit");
            logger.close();
            System.exit(0);
        }

    }


}
