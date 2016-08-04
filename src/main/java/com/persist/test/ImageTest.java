package com.persist.test;

import com.persist.util.helper.ImageHepler;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;

/**
 * Created by taozhiheng on 16-8-1.
 */
public class ImageTest {

    public static void main(String[] args) throws Exception
    {
        ImageHepler.saveImageAsJpg("/home/taozhiheng/1.jpg", "/home/taozhiheng/1-scale.jpg",
                227, 227, false);
    }
}
