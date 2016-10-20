package com.persist.util.helper;

import java.awt.*;
import java.awt.geom.AffineTransform;
import java.awt.image.AffineTransformOp;
import java.awt.image.BufferedImage;

/**
 * Created by taozhiheng on 16-10-20.
 *
 * rotate and resize BufferedImage
 *
 * author: persist
 */
public class BufferedImageHelper {

    /**
     *
     * @param image the source image
     * @param degree the rotate degree [-180, 180]
     * @return the destination image
     */
    public static BufferedImage rotate(BufferedImage image, int degree) {
        int iw = image.getWidth();
        int ih = image.getHeight();
        int w = 0;
        int h = 0;
        int x = 0;
        int y = 0;
        degree = degree % 360;
        if (degree < 0)
            degree = 360 + degree;// ??????0-360???
        double ang = degree * 0.0174532925;// ???????

        if (degree == 180 || degree == 0 || degree == 360) {
            w = iw;
            h = ih;
        } else if (degree == 90 || degree == 270) {
            w = ih;
            h = iw;
        } else {
            int d = iw + ih;
            w = (int) (d * Math.abs(Math.cos(ang)));
            h = (int) (d * Math.abs(Math.sin(ang)));
        }
        x = (w / 2) - (iw / 2);
        y = (h / 2) - (ih / 2);
        BufferedImage rotatedImage = new BufferedImage(w, h, image.getType());
        Graphics gs = rotatedImage.getGraphics();
        gs.fillRect(0, 0, w, h);
        AffineTransform at = new AffineTransform();
        at.rotate(ang, w / 2, h / 2);
        at.translate(x, y);
        AffineTransformOp op = new AffineTransformOp(at,
                AffineTransformOp.TYPE_BILINEAR);
        op.filter(image, rotatedImage);
        image = rotatedImage;
        return image;
    }

    /**
     * @param image the source image
     * @param width the destination width
     * @param height the destination height
     * @return the destination image
     * */
    public static BufferedImage resize(BufferedImage image, int width, int height)
    {   //resize our image and preserve hard edges which we need for pixel art
        int imageWidth  = image.getWidth();
        int imageHeight = image.getHeight();
        double scaleX = (double)width/imageWidth;
        double scaleY = (double)height/imageHeight;
        AffineTransform scaleTransform = AffineTransform.getScaleInstance(scaleX, scaleY);
        AffineTransformOp op = new AffineTransformOp(scaleTransform, AffineTransformOp.TYPE_NEAREST_NEIGHBOR);
        return op.filter(image, new BufferedImage(width, height, image.getType()));
    }

}
