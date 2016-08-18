package com.persist.util.helper;


import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import sun.misc.BASE64Decoder;
import java.io.*;
import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;

/**
 * Created by taozhiheng on 16-7-12.
 * helper to handle local file
 */
public class FileHelper {


    /**
     * download image from url
     * */
    public static boolean download(OutputStream os, String urlString)
    {
        if(urlString == null)
            return false;
        try {
            URL url = new URL(urlString);
            URLConnection conn = url.openConnection();
            InputStream is = conn.getInputStream();
            byte[] buf = new byte[1024];
            int size;
            while((size = is.read(buf))>-1)
            {//循环读取
                os.write(buf, 0, size);
            }
            os.flush();
            is.close();
            return true;
        }
        catch (MalformedURLException e)
        {
            e.printStackTrace();
            return false;
        }
        catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * parse properties from json string
     * support properties:
     * int
     * String
     * String[]
     * */
    public static void setConfig(Object config, String content)
    {
        try {
            JSONParser parser = new JSONParser();
            JSONObject object = (JSONObject) parser.parse(content);
            setConfig(config, object);
        } catch (ParseException e)
        {
            e.printStackTrace();
        }
    }

    public static void setConfig(Object config, JSONObject object)
    {
        try {
            Field[] fields = config.getClass().getDeclaredFields();
            Object o;
            for(Field field: fields)
            {
                field.setAccessible(true);
                o = object.get(field.getName());
                if(o != null) {
                    if(o instanceof Long)
                        field.set(config, Integer.parseInt(o.toString()));
                    else if(o instanceof JSONArray)
                    {
                        String[] array = new String[((JSONArray) o).size()];
                        ((JSONArray) o).toArray(array);
                        field.set(config, array);
                    }
                    else {
                        field.set(config, o);
                    }
                }
                System.out.println(field.getName()+" -> "+field.get(config));
            }

        }
        catch (IllegalAccessException e)
        {
            e.printStackTrace();
        }

    }

    public static String readString(String filePath)
    {
        String text = "";
        File file = new File(filePath);
        try {
            FileInputStream fis = new FileInputStream(file);
            BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
            char[] buffer= new char[512];
            int count;
            StringBuilder builder = new StringBuilder();
            while((count = reader.read(buffer)) != -1)
            {
                builder.append(buffer, 0, count);
            }
            text = builder.toString();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        return text;
    }

    public static byte[] base64Decode(String src)
    {
        if(src == null)
            return null;
        byte[] data;
        BASE64Decoder decoder = new BASE64Decoder();
        try {
            data = decoder.decodeBuffer(src);
            return data;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
