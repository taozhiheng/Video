package com.persist.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Scanner;

/**
 * Created by taozhiheng on 16-8-17.
 *
 */
public class FileTest {

    public static void main(String[] args) throws Exception
    {
        File file = new File(args[0]);
        int step = 10;
        if(args.length > 1)
            step = Integer.parseInt(args[1]);
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
        String string;
        Scanner scanner = new Scanner(System.in);
        String cmd;
        int i = 0;
        while(true)
        {
            string = reader.readLine();
            System.out.println(string);
            i++;
            if(i == step)
            {
                cmd = scanner.next();
                if ("quit".equals(cmd))
                    break;
                i = 0;
            }
        }
        scanner.close();
        reader.close();
    }
}
