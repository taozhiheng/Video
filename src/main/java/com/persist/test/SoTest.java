package com.persist.test;

import com.persist.bean.analysis.CalculateInfo;
import com.persist.util.tool.analysis.Predict;
import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by taozhiheng on 16-8-5.
 *
 */
public class SoTest {

    public static void main(String[] args) throws Exception
    {
        Predict.init(args[0]);
        System.out.println("load ok");

        List<CalculateInfo> list = new ArrayList<CalculateInfo>();
        CalculateInfo calculateInfo;
        int size = args.length;
        byte[] pixels;
        for(int i = 1; i < size*100; i++)
        {
            BufferedImage image = null;
            try {
                image = ImageIO.read(new File(args[i%(size-1)+1]));
                pixels = ((DataBufferByte) image.getRaster().getDataBuffer()).getData();
                calculateInfo = new CalculateInfo("url-"+i+"-"+args[i%(size-1)+1], pixels, image.getHeight(), image.getWidth());
                list.add(calculateInfo);

                long start = System.currentTimeMillis();
                System.out.println("add ok, start triggerPredict at "+start);
                Map<String, Float> map = Predict.predictProxy(list);
                long stop = System.currentTimeMillis();
                System.out.println("triggerPredict finish at "+stop);
                System.out.println(map.size());
                for (Map.Entry<String, Float> entry : map.entrySet()) {
                    System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
                }
                System.out.println("time="+(stop-start)+" ms, size="+map.size());

            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }
}
