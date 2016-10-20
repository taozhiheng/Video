package com.persist.util.tool.image;

import com.persist.bean.image.ImageInfo;
import com.persist.util.helper.FileLogger;
import com.persist.util.helper.HBaseHelper;

/**
 * Created by taozhiheng on 16-10-14.
 *
 */
public class RecorderImpl implements IRecorder {

    private final static String TAG = "RecorderImpl";

    private HBaseHelper mHelper;
    private String quorum;
    private int port;
    private String master;
    private String auth;

    private String usageTable;
    private String usageFamily;
    private String[] usageColumns;

    private String recentTable;
    private String recentFamily;
    private String[] recentColumns;

    private FileLogger mLogger;

    public RecorderImpl(String quorum, int port, String master, String auth,
                        String usageTable, String usageFamily, String[] usageColumns,
                        String recentTable, String recentFamily, String[] recentColumns)
    {
        if(quorum == null || master == null)
            throw new RuntimeException("HBase quorum or master must not be null");
        this.quorum = quorum;
        this.port = port;
        this.master = master;
        this.auth = auth;
        this.usageTable = usageTable;
        this.usageFamily = usageFamily;
        this.usageColumns = usageColumns;
        this.recentTable = recentTable;
        this.recentFamily = recentFamily;
        this.recentColumns = recentColumns;
    }

    private void initHBase()
    {
        if(mHelper != null)
            return;
        mHelper = new HBaseHelper(quorum, port, master, auth);
        try {
            mHelper.createTable(usageTable, new String[]{usageFamily});
            mHelper.createTable(recentTable, new String[]{recentFamily});
        } catch (Exception e) {
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

    public void setLogger(FileLogger logger) {
        this.mLogger = logger;
    }

    public boolean record(String user, int size, String urls, String values) {
        boolean ok = false;
        if(user == null)
        {
            if(mLogger != null)
            {
                mLogger.log(TAG, "Warning: user is null! urls:"+urls+", values:"+values);
                mLogger.getPrintWriter().flush();
            }
            return false;
        }
        if(urls == null || values == null)
            return false;

        if(mHelper == null)
            initHBase();

        if(mHelper != null)
        {
            try
            {
                //insert a record to usageTable
                String key = user+"-"+System.currentTimeMillis();
                mHelper.addRow(usageTable, key, usageFamily, usageColumns,
                        new String[]{user, String.valueOf(size)});
                //insert or update a record in recentTable
                mHelper.addRow(recentTable, user, recentFamily, recentColumns,
                        new String[]{urls, values});
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
