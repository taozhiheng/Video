package com.persist.test;

import com.persist.util.helper.BufferedImageHelper;

import javax.imageio.ImageIO;
import java.awt.geom.AffineTransform;
import java.awt.image.AffineTransformOp;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

/**
 * Created by taozhiheng on 16-10-20.
 */
public class BufferedImageTest {

    public static BufferedImage resize(BufferedImage source, int targetW, int targetH)
    {
        int w = source.getWidth();
        int h = source.getHeight();
        double sx = (double) targetW / w;
        double sy = (double) targetH / h;
        BufferedImage target = new BufferedImage(targetW, targetH, source.getType());
        AffineTransform at = new AffineTransform();
        at.scale(sx, sy);
        AffineTransformOp op = new AffineTransformOp(at, AffineTransformOp.TYPE_BILINEAR);
        op.filter(source, target);
        return target;
    }

    public static void main(String[] args) throws Exception
    {
        if(args.length < 2)
            throw new RuntimeException("ImageTest needs at least 2 arguments like: srcImageFile dstImageFile");
        String inFilePath = args[0];
        String outFilePath = args[1];
        File file = new File(inFilePath);
        InputStream in = new FileInputStream(file);
        File saveFile = new File(outFilePath);
        String fileName = saveFile.getName();
        String formatName = fileName.substring(fileName
                .lastIndexOf('.') + 1);
        BufferedImage srcImage = ImageIO.read(in);
        BufferedImage dstImage = BufferedImageHelper.resize(srcImage, 227, 227);
        ImageIO.write(dstImage, formatName, saveFile);
    }

}
