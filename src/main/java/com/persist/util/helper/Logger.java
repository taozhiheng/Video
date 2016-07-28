package com.persist.util.helper;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Serializable;

/**
 * Created by zhiheng on 2016/7/5.
 * helper to log
 */
public class Logger implements Serializable{

    private static PrintWriter printWriter = new PrintWriter(System.out);
    private static boolean isDebug = true;

    public static void setOutput(OutputStream out)
    {
        printWriter = new PrintWriter(out);
    }

    public static void setDebug(boolean debug)
    {
        isDebug = debug;
    }

    public static boolean isDebug()
    {
        return isDebug;
    }

    public static void log(String tag, String msg)
    {
        if(printWriter != null && isDebug) {
            printWriter.println(System.currentTimeMillis()+" [" + tag + "]:\t" + msg);
            printWriter.flush();
        }
    }

    public static void close()
    {
        printWriter.close();
    }
}
