package com.persist.test;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.persist.bean.analysis.AnalysisConfig;
import com.persist.bean.analysis.ImageInfo;
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

        AnalysisConfig config = new AnalysisConfig();
        Gson gson = new Gson();
        config = gson.fromJson(FileHelper.readString("analyzer_config.json"), AnalysisConfig.class);
        System.out.println("warnValue:"+config.warnValue);
        System.out.println(System.getProperty("java.library.path"));

        try {
            ImageInfo info = gson.fromJson("aaa", ImageInfo.class);
        }
        catch(JsonSyntaxException e)
        {
            System.out.println("JsonSyntaxException");
            e.printStackTrace();
        }

        ByteArrayOutputStream os = new ByteArrayOutputStream();
        System.out.println(FileHelper.download(os, "aaa"));
    }
}
