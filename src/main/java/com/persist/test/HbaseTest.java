package com.persist.test;

import com.persist.util.helper.HBaseHelper;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;
import java.util.Scanner;

/**
 * Created by taozhiheng on 16-7-30.
 *
 */
public class HbaseTest {

    public static void main(String[] args)
    {
        if(args.length < 2)
            throw new RuntimeException("HbaseTest needs at least 2 arguments: quorum, port");
        String quorum = args[0];
        int port = Integer.parseInt(args[1]);
        HBaseHelper helper = new HBaseHelper(quorum, port);
        List<String> list = helper.getAllTables();
        if(list != null && list.size() > 0) {
            for (String s : list)
                System.out.println(s);
        }
        try {
            Scanner scanner = new Scanner(System.in);
            System.out.println("which table do you want to query?");
            String table = scanner.next();
            System.out.println("which row do you want to query in table "+table+"?");
            String row = scanner.next();
            Result result = helper.getRow(table, row);
            if(result == null)
            {
                System.out.println("No such row");
            }
            else
            {
                System.out.println("Please input column family and qualifier:");
                String family = scanner.next();
                String column = scanner.next();
                scanner.close();
                byte[] data = result.getValue(Bytes.toBytes(family), Bytes.toBytes(column));
                System.out.println("Value: "+Bytes.toString(data));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
