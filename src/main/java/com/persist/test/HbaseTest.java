package com.persist.test;

import com.persist.util.helper.HBaseHelper;
import java.util.List;

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
            helper.createTable("test", new String[]{"test-family"});
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
