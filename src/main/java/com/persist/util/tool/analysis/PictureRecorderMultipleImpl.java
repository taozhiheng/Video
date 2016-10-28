package com.persist.util.tool.analysis;

import com.persist.bean.analysis.PictureResult;
import com.persist.util.helper.FileLogger;
import com.persist.util.helper.HBaseHelper;
import java.util.Calendar;

/**
 * Created by tl on 16-7-28.
 *
 */
public class PictureRecorderMultipleImpl implements IPictureRecorder {

    private final static String TAG = "PictureRecorderImpl";

    private HBaseHelper mHelper;
    private String quorum;
    private int port;
    private String master;
    private String auth;

    private String tableName;
    private String yellowTableName;
    private String columnFamily;
    private String[] columns;

    private FileLogger mLogger;


    public PictureRecorderMultipleImpl(String quorum, int port, String master, String auth,
                                           String tableName, String yellowTableName, String columnFamily, String[] columns)
    {
        if(quorum == null || master == null)
            throw new RuntimeException("HBase quorum or master must not be null");
        this.quorum = quorum;
        this.port = port;
        this.master = master;
        this.auth = auth;
        this.tableName = tableName;
        this.yellowTableName = yellowTableName;
        this.columnFamily = columnFamily;
        this.columns = columns;
    }

    private void initHBase()
    {
        if(mHelper != null)
            return;
        mHelper = new HBaseHelper(quorum, port, master, auth);
        try {

            mHelper.createTable(tableName, new String[]{columnFamily});
            //把所有是黄图的记录添加到同一张表中
            mHelper.createTable(yellowTableName, new String[]{columnFamily});
        } catch (Exception e) {
//            e.printStackTrace();
            if(mLogger != null)
            {
                e.printStackTrace(mLogger.getPrintWriter());
                mLogger.getPrintWriter().flush();
            }
        }
    }


    public void prepare() {
        initHBase();
    }

    public void setLogger(FileLogger logger)
    {
        this.mLogger = logger;
    }

    public boolean recordResult(PictureResult result) {
        boolean ok = false;

        if(result == null || result.description == null)
            return false;

        if(mHelper == null)
            initHBase();

        if(mHelper != null )
        {
            try {
                if(mLogger != null)
                    mLogger.log(TAG, "write table "+tableName);
                mHelper.addRow(tableName, result.description.url, columnFamily,columns,
                        new String[]{result.description.video_id, result.description.time_stamp,
                                String.valueOf(result.ok), String.valueOf(result.percent)});

                //根据不同的视频id来创建不同的表
                String urlTable = String.valueOf(Math.abs(result.description.video_id.hashCode()));
                mHelper.createTable(urlTable, new String[]{columnFamily});
                if(mLogger != null)
                    mLogger.log(TAG, "write table "+urlTable);
                mHelper.addRow(urlTable, result.description.url, columnFamily,columns,
                        new String[]{result.description.video_id, result.description.time_stamp,
                                String.valueOf(result.ok), String.valueOf(result.percent)});
                //record unhealthy images
                if(!result.ok)
                {
                    mHelper.addRow(yellowTableName, result.description.url, columnFamily,columns,
                            new String[]{result.description.video_id, result.description.time_stamp,
                                    String.valueOf(false), String.valueOf(result.percent)});
                    if(mLogger != null)
                    {
                        mLogger.log(TAG, "record yellow image:"+result.description.url);
                    }
                }
                //将记录按小时写入到不同的表中
                long time = Long.valueOf(result.description.time_stamp);

                Calendar calendar = Calendar.getInstance();
                calendar.setTimeInMillis(time);

                int year = calendar.get(Calendar.YEAR);
                int month = calendar.get(Calendar.MONTH)+1;
                int date = calendar.get(Calendar.DATE);
                int hour = calendar.get(Calendar.HOUR_OF_DAY);
                String time_table_name = year+"-"+month+"-"+date+"-"+hour;

                mHelper.createTable(time_table_name, new String[]{columnFamily});

                if(mLogger != null)
                    mLogger.log(TAG, "write table "+time_table_name);
                mHelper.addRow(time_table_name, result.description.url, columnFamily,columns,
                        new String[]{result.description.video_id, result.description.time_stamp,
                                String.valueOf(result.ok), String.valueOf(result.percent)});
                ok = true;

            } catch (Exception e) {
                if(mLogger != null)
                {
                    e.printStackTrace(mLogger.getPrintWriter());
                    mLogger.getPrintWriter().flush();
                }
            }
        }
        return ok;
    }

    public void stop() {
        if(mHelper != null)
        {
            mHelper.close();
            mHelper = null;
        }
    }
}