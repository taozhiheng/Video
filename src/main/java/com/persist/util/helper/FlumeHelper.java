package com.persist.util.helper;

import java.io.IOException;

/**
 * Created by taozhiheng on 16-7-22.
 *
 */
public class FlumeHelper {


    public static Process startFlumeProcess(String url, String dir)
    {
        try {
            //execute a command or a shell script
            return Runtime.getRuntime().exec("");
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

}
