package com.persist.test;

import com.google.gson.Gson;
import com.persist.bean.analysis.AnalysisConfig;
import com.persist.bean.grab.GrabConfig;
import com.persist.bean.grab.VideoInfo;
import com.persist.util.helper.FileHelper;
import com.persist.util.helper.HBaseHelper;
import com.persist.util.helper.Logger;
import com.persist.util.tool.grab.IVideoNotifier;
import com.persist.util.tool.grab.VideoNotifierImpl;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import redis.clients.jedis.Jedis;

import java.io.*;
import java.lang.reflect.Field;

/**
 * Created by taozhiheng on 16-7-20.
 *
 */
public class Test {

    private final static String TAG = "Test";

    public static void main(String[] args) throws Exception
    {
        //the notifier is just for test, actually it can be removed

        StringBuilder builder = new StringBuilder();
        String host = null;
        String password = null;
        int port = 0;
        String url = null;
        String dir = null;
        builder.append(' ').append(host).append(' ').append(port).append(' ').append(password);
        builder.append(' ').append(url).append(' ').append(dir);
        IVideoNotifier notifier = new VideoNotifierImpl(
                "null", port,
                "null", new String[]{url});
        System.out.println(builder.toString());
        notifier.notify("msg");
        notifier.notify("test");
    }
}
