package com.persist.test;

import com.persist.bean.analysis.CalculateInfo;
import com.persist.util.tool.analysis.CalculatorImpl;

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

    public static void main(String[] args)
    {
        CalculatorImpl calculator = new CalculatorImpl(args[0]);
        calculator.prepare();
        System.out.println("load ok");

        List<CalculateInfo> list = new ArrayList<CalculateInfo>();
        CalculateInfo calculateInfo;
        int size = args.length;
        byte[] pixels;
        for(int i = 1; i < size; i++)
        {
            BufferedImage image = null;
            try {
                image = ImageIO.read(new File(args[i]));
                pixels = ((DataBufferByte) image.getRaster().getDataBuffer()).getData();
                calculateInfo = new CalculateInfo("url-"+i+"-"+args[i], pixels, image.getHeight(), image.getWidth());
                list.add(calculateInfo);

                System.out.println("add ok");
                Map<String, Float> map = calculator.predict(list);
                System.out.println("predict ok!");
                System.out.println(map.size());
                for (Map.Entry<String, Float> entry : map.entrySet()) {
                    System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
                }
                System.out.println();

            } catch (IOException e) {
                e.printStackTrace();
            }

        }

    }
}
