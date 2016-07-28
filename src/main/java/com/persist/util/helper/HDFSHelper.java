package com.persist.util.helper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import java.io.*;
import java.net.URI;
import java.lang.String;

/**
 * Created by taozhiheng on 16-7-17.
 * simple operations on hdfs
 */
public class HDFSHelper {

    private String ip;

    public HDFSHelper(String ip)
    {
        this.ip = ip;
    }

    /**
     * upload local file or directory to remote hdfs system
     * @param local the local file or directory path
     * @param remote the remote hdfs system absolute file or directory path
     * */
    public boolean upload(String local, String remote)
    {
        return upload(new File(local), remote);
    }

    /**
     * upload local file or directory to remote hdfs system
     * @param file the local file or directory
     * @param remote the remote hdfs system absolute file or directory path
     * */
    public boolean upload(File file, String remote)
    {
        if(!file.exists())
            return false;
        if(file.isFile())
        {
            try {
                //just upload the file to hdfs (remote)
                return upload(new BufferedInputStream(new FileInputStream(file)), remote);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                return false;
            }
        }
        else
        {
            File[] files = file.listFiles();

            if(files != null)
            {
                int size = files.length;
                //upload each file to hads (remote + "/" + f.getName())
                for (File f : files) {
                    if(upload(f, remote + File.separator + f.getName()))
                        size--;
                }
                return size == 0;
            }
            return false;
        }
    }

    /**
     * upload local file or directory to remote hdfs system
     * @param is the input stream of local file
     * @param remote the remote hdfs system absolute file path
     * */
    public boolean upload(InputStream is, String remote)
    {
        String dst = ip +File.separator+ remote;
        Configuration conf = new Configuration();
        try
        {
            FileSystem fs = FileSystem.get(URI.create(dst), conf);
            OutputStream out = fs.create(new Path(dst), new Progressable() {
                public void progress() {
                    System.out.print(".");
                }
            });
            IOUtils.copyBytes(is, out, 4096, true);
            return true;
        }
        catch (IOException e)
        {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * download file from remote hdfs systemt to local file
     * @param local the local file path
     * @param remote the remote hdfs system absolute file path
     * */
    public boolean download(String local,String remote)
    {
        return download(new File(local), remote);
    }

    /**
     * download file from remote hdfs systemt to local file
     * @param file the local file
     * @param remote the remote hdfs system absolute file path
     * */
    public boolean download(File file, String remote)
    {
        if(file.exists() && file.isFile()) {
            try {
                return download(new FileOutputStream(file), remote);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                return false;
            }
        }
        return false;
    }

    /**
     * download file from remote hdfs systemt to local file
     * @param os the output stream of the local file
     * @param remote the remote hdfs system absolute file path
     * */
    public boolean download(OutputStream os, String remote)
    {
        String dst = ip +File.separator+ remote;
        Configuration conf = new Configuration();
        try {
            FileSystem fs = FileSystem.get(URI.create(dst), conf);
            FSDataInputStream fsDataInputStream = fs.open(new Path(dst));
            byte[] ioBuffer = new byte[1024];
            int readLen = fsDataInputStream.read(ioBuffer);
            while (-1 != readLen) {
                os.write(ioBuffer, 0, readLen);
                readLen = fsDataInputStream.read(ioBuffer);
            }
            os.close();
            fsDataInputStream.close();
            fs.close();
            return true;
        }
        catch (IOException e)
        {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * delete file in remote hdfs system
     * @param remote the remote hdfs system absolute file path
     * */
    private  boolean delete(String remote)
    {
        String dst = ip +File.separator+remote;
        Configuration conf = new Configuration();
        FileSystem fs;
        try {
            fs = FileSystem.get(URI.create(dst), conf);
            fs.deleteOnExit(new Path(dst));
            fs.close();
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }
}
