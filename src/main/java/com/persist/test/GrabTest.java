package com.persist.test;

import org.bytedeco.javacv.FFmpegFrameGrabber;
import org.bytedeco.javacv.Frame;

/**
 * Created by taozhiheng on 16-8-18.
 *
 */
public class GrabTest {

    public static void main(String[] args) throws Exception
    {
//        String url = args[0];
        String url = "rtmp://124.139.232.61:1935/live/livestream";
        FFmpegFrameGrabber grabber = new FFmpegFrameGrabber(url);
        grabber.start();
        int expectNumber = 0;
        int grabStep = 1000000;
        int i = 0;
        Frame frame;
        int frameNumber = 0;
        while(true)
        {
            if(i > 0)
            {
                expectNumber += grabStep;
                grabber.setFrameNumber(expectNumber);
                System.out.println("expectNumber="+expectNumber);
            }
            frame = grabber.grabImage();
            frameNumber = grabber.getFrameNumber();
            System.out.println("frameNumber="+frameNumber);
            System.out.println("frame is null?"+(frame == null));
            if(frame != null)
                System.out.println("image is null?"+(frame.image == null));
            i++;
        }

    }
}
