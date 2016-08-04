package com.persist.util.helper;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.Calendar;

/**
 * Created by zhiheng on 2016/7/5.
 * helper to log
 */
public class Logger implements Serializable{

    private static PrintWriter printWriter = new PrintWriter(System.out);

    private static Calendar calendar = Calendar.getInstance();

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
        if(printWriter != null && isDebug)
        {
            calendar.setTimeInMillis(System.currentTimeMillis());
            //print date
            printWriter.print(String.format("%04d/%02d/%02d ", calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH)+1, calendar.get(Calendar.DATE)));
            //print time
            printWriter.print(String.format("%02d:%02d:%02d ", calendar.get(Calendar.HOUR_OF_DAY), calendar.get(Calendar.MINUTE), calendar.get(Calendar.SECOND)));
            printWriter.println("[" + tag + "]: " + msg);
            printWriter.flush();
        }
    }

    public static void close()
    {
        printWriter.close();
    }
}
