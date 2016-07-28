package com.persist.util.tool.analysis;

import com.persist.bean.analysis.PictureKey;
import com.persist.bean.analysis.PictureResult;
import com.persist.util.helper.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import sun.misc.BASE64Decoder;
import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Created by zhiheng on 2016/7/5.
 * hold a Jedis instance
 * before starting working, the method prepare() must be invoked
 *
 * read image data from redis, and judge whether the image is ok
 */
public class PictureCalculatorImpl implements IPictureCalculator {

    private final static String TAG = "PictureCalculatorImpl";
    private final static String URL = "http://www.faceplusplus.com.cn/wp-content/themes/faceplusplus/assets/img/demo/9.jpg";

    private Jedis mJedis;
    private String host;
    private int port;
    private String password;

    public PictureCalculatorImpl(String host, int port, String password)
    {
        if(host == null || password == null)
            throw new RuntimeException("Redis host or password must not be null");
        this.host = host;
        this.port = port;
        this.password = password;
    }


    private void initJedis()
    {
        if(mJedis != null)
            return;
        JedisPool pool = null;
        try {
            // JedisPool依赖于apache-commons-pools1
            JedisPoolConfig config = new JedisPoolConfig();
            pool = new JedisPool(config, host, port, 3000, password);
            System.out.println(pool);
            mJedis = pool.getResource();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Jedis getJedis()
    {
        return mJedis;
    }

    public void prepare() {
        initJedis();
    }

    public PictureResult calculateImage(PictureKey pictureKey) {
        //read image data from redis
        if(mJedis == null) {
            initJedis();
        }
        String data = mJedis.get(URL);
        byte[] bytes = base64Decode(data);
        if(bytes == null)
            return new PictureResult(pictureKey, false, -1);
        //calculate
        InputStream is = new ByteArrayInputStream(bytes);
        BufferedImage image = null;
//        try {
//            image = ImageIO.read(is);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        if(image == null)
//            return new PictureResult(pictureKey, false, -1);
        image = new BufferedImage(1080, 720, BufferedImage.TYPE_3BYTE_BGR);

        int width = image.getWidth();
        int height = image.getHeight();
        int minX = image.getMinX();
        int minY = image.getMinY();
        int[] rgb = new int[3];
        for (int i = minX; i < width; i++) {
            for (int j = minY; j < height; j++) {
                int pixel = image.getRGB(i, j);
                rgb[0] = (pixel & 0xff0000) >> 16;
                rgb[1] = (pixel & 0xff00) >> 8;
                rgb[2] = (pixel & 0xff);
            }
        }
        Logger.log(TAG, "calculate "+pictureKey.url);
        return new PictureResult(pictureKey, (rgb[0] > rgb[1]) && (rgb[1] < rgb[2]), 0.98f);
    }

    public static byte[] base64Decode(String src)
    {
        if(src == null)
            return null;
        byte[] data = null;
        BASE64Decoder decoder = new BASE64Decoder();
        try {
            data = decoder.decodeBuffer(src);
            return data;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
