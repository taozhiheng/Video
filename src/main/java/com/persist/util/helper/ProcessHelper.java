package com.persist.util.helper;

import java.io.*;

/**
 * Created by taozhiheng on 16-7-21.
 *
 * send message from process to children processes
 *
 */
public class ProcessHelper {

    /**
     * send message to process
     * */
    public static void sendMessage(Process process, String msg)
    {
        if(process == null)
            return;
        PrintWriter writer = new PrintWriter(new OutputStreamWriter(process.getOutputStream()));
        writer.println(msg);
        writer.flush();
    }

    /**
     * close message pipe
     * */
    public static void finishMessage(Process process)
    {
        OutputStream os = process.getOutputStream();
        try {
            os.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
