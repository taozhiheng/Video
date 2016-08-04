package com.persist.test;

import com.persist.util.helper.FileHelper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

/**
 * Created by taozhiheng on 16-8-1.
 *
 */
public class RuntimeTest {


    private final static String STORM_HOME = "STORM_HOME";

    public static void main(String[] args)
    {
        String file = "path.txt";
        try {

            Map<String, String> map = System.getenv();
            String value = map.get(STORM_HOME);
            System.out.println(STORM_HOME+"="+value);

            String cmd = FileHelper.readString(file);
            cmd = cmd.replace("$STORM_HOME", value);
            System.out.println(cmd);

            Process p = Runtime.getRuntime().exec(cmd, new String[]{STORM_HOME+"="+value});
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
