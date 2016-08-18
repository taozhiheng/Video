package com.persist;

import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.utils.DRPCClient;
import com.google.gson.Gson;
import com.persist.bean.image.ImageInfo;
import com.persist.util.helper.FileHelper;
import com.persist.util.helper.Logger;


/**
 * Created by taozhiheng on 16-8-4.
 *
 * query a picture specified by url
 *
 * input url like:
 * {"urls": ["url1", "url2", "url3"]}
 *
 */
public class ImageQuery {

    public static void main(String[] args) throws Exception
    {
        if(args.length < 4)
            throw new RuntimeException("ImageQuery needs at least 4 arguments.You should input like:" +
                    "host port function file");
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        String func = args[2];
        DRPCClient client = new DRPCClient(host, port);
        Gson gson = new Gson();
        String[] urls = gson.fromJson(FileHelper.readString(args[3]), String[].class);
        if(urls == null || urls.length <= 0)
        {
            System.out.println("Invalid urls, please check you input file: "+args[3]);
            return;
        }
        String result;
        try {
            ImageInfo info = new ImageInfo(urls, null, null);
            long start = System.currentTimeMillis();
            Logger.log("ImageQuery", "start query");
            result = client.execute(func, gson.toJson(info));
            Logger.log("ImageQuery", "end query");
            long end = System.currentTimeMillis();
            System.out.println("Query size="+urls.length+", time="+(end-start)+" ms");
            System.out.println("The result is: ");
            System.out.println(result);
        }catch (DRPCExecutionException e)
        {
            e.printStackTrace();
        }
    }
}
