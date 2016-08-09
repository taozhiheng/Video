package com.persist;

import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.utils.DRPCClient;

/**
 * Created by taozhiheng on 16-8-4.
 * input url like:
 * {"url":""}
 *
 */
public class ImageQuery {

    public static void main(String[] args) throws Exception
    {
        if(args.length < 4)
            throw new RuntimeException("ImageQuery needs at least 4 arguments.You should input like:" +
                    "host port function url1 url2 ...");
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        String func = args[2];
        DRPCClient client = new DRPCClient(host, port);
        String result;
        for(int i = 3; i < args.length; i++)
        {
            System.out.println("Query "+args[i]);
            try {
                result = client.execute(func, "{\"url\":\"" + args[i] + "\"}");
                System.out.println("The result of " + args[i] + ":");
                System.out.println(result);
            }catch (DRPCExecutionException e)
            {
                e.printStackTrace();
            }
        }
    }
}
