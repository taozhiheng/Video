package com.persist.test;

import com.persist.util.helper.ImageHelper;

/**
 * Created by taozhiheng on 16-8-1.
 *
 */
public class ImageTest {

    public static void main(String[] args) throws Exception
    {
        if(args.length < 2)
            throw new RuntimeException("ImageTest needs at least 2 arguments like: srcImageFile dstImageFile");
        ImageHelper.saveImageAsJpg(args[0], args[1],
                227, 227, false);
    }
}
