package com.persist.test;

import com.persist.util.helper.FileHelper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Created by taozhiheng on 16-8-1.
 *
 */
public class RuntimeTest {

    public static void main(String[] args)
    {
        String file = args[0];
        try {
            String cmd = FileHelper.readString(file);
            System.out.println(cmd);
            Process p = Runtime.getRuntime().exec(cmd);
            BufferedReader reader = new BufferedReader(new InputStreamReader(p.getErrorStream()));
            String msg;
            while(true)
            {
                msg = reader.readLine();
                System.out.println(msg);
                if(msg == null)
                    break;
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
