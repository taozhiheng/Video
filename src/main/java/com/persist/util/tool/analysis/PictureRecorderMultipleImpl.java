package com.persist.util.tool.analysis;

import com.persist.bean.analysis.PictureResult;
import com.persist.util.helper.HBaseHelper;
import com.persist.util.helper.Logger;

import java.util.Calendar;

/**
 * Created by tl on 16-7-28.
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
        if(mHelper == null)
        {
            mHelper = new HBaseHelper(quorum, port, master, auth);
            try {

                mHelper.createTable(tableName, new String[]{columnFamily});
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    public void prepare() {
        initHBase();
    }

    public boolean recordResult(PictureResult result) {
        boolean ok = false;

        if(mHelper != null)
        {
            mHelper = new HBaseHelper(quorum, port, master, auth);
            try {
                mHelper.addRow(tableName, result.description.url, columnFamily,columns,
                        new String[]{result.description.video_id, result.description.time_stamp,
                                String.valueOf(result.ok), String.valueOf(result.percent)});

                //根据不同的视频id来创建不同的表
                mHelper.createTable(result.description.video_id, new String[]{columnFamily});
                mHelper.addRow(result.description.video_id, result.description.url, columnFamily,columns,
                        new String[]{result.description.video_id, result.description.time_stamp,
                                String.valueOf(result.ok), String.valueOf(result.percent)});

                //把所有是黄图的记录添加到同一张表中
                mHelper.createTable(yellowTableName, new String[]{columnFamily});
                if(result.ok)
                {
                    mHelper.addRow(yellowTableName, result.description.url, columnFamily,columns,
                            new String[]{result.description.video_id, result.description.time_stamp,
                                    String.valueOf(true), String.valueOf(result.percent)});
                }
                //将记录按小时写入到不同的表中
                long time = Long.valueOf(result.description.time_stamp);

                Calendar calendar = Calendar.getInstance();
                calendar.setTimeInMillis(time);

                int year = calendar.get(Calendar.YEAR);
                int month = calendar.get(Calendar.MONTH);
                int date = calendar.get(Calendar.DATE);
                int hour = calendar.get(Calendar.HOUR_OF_DAY);
                String time_table_name = year+"-"+month+"-"+date+"-"+hour;

                mHelper.createTable(time_table_name, new String[]{columnFamily});

                mHelper.addRow(time_table_name, result.description.url, columnFamily,columns,
                        new String[]{result.description.video_id, result.description.time_stamp,
                                String.valueOf(result.ok), String.valueOf(result.percent)});

                ok = true;

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        //write back
        Logger.log(TAG, "write result:"
                + result.description.url + ", "
                + result.description.video_id + ", "
                + result.ok + ", "
                + result.percent);
        return ok;
    }

    public void stop() {
        mHelper.close();
    }
}