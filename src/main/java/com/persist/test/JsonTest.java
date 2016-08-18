package com.persist.test;

import com.google.gson.Gson;
import com.persist.util.helper.FileHelper;
import com.persist.util.helper.FileLogger;

/**
 * Created by taozhiheng on 16-8-15.
 *
 */
public class JsonTest {

    public static void main(String[] args)
    {
        if(args.length < 1)
            throw new RuntimeException("ImageTest needs at least 1 arguments like: jsonStringArrayFile");
        Gson gson = new Gson();
        String[] urls = gson.fromJson(FileHelper.readString(args[0]), String[].class);
        for(String url : urls)
        {
            System.out.println(url);
        }
        FileLogger logger = new FileLogger("trace");
        int[] a;
        for(int i = 0; i < 10; i++) {
            try {
                a = new int[2];
                System.out.println("Access element three :" + a[3]);
            } catch (ArrayIndexOutOfBoundsException e) {
                e.printStackTrace(logger.getPrintWriter());
                logger.getPrintWriter().flush();
                e.printStackTrace(System.out);
            }
            System.out.println("Out of the block 1");
            try {
                a = new int[2];
                System.out.println("Access element three :" + a[3]);
            } catch (ArrayIndexOutOfBoundsException e) {
                e.printStackTrace(logger.getPrintWriter());
                logger.getPrintWriter().flush();
                e.printStackTrace(System.out);
            }
            System.out.println("Out of the block 2");
            System.out.println("Index="+i);
        }
    }

}
