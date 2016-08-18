package com.persist.test;

import java.util.concurrent.TimeUnit;

/**
 * Created by taozhiheng on 16-7-20.
 *
 */
public class ProcessTest {

    private final static String TAG = "Test";

    public static void main(String[] args) throws Exception
    {
        Process process = Runtime.getRuntime().exec("ls");
        boolean hasExit = process.waitFor(-1, TimeUnit.SECONDS);
        while(!hasExit)
        {
            System.out.println("not exit yet");
            hasExit = process.waitFor(-1, TimeUnit.SECONDS);
        }
        System.out.println("exit");
    }
}
