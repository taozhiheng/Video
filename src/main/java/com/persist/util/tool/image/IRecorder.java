package com.persist.util.tool.image;

import com.persist.util.helper.FileLogger;

import java.io.Serializable;

/**
 * Created by taozhiheng on 16-10-14.
 *
 */
public interface IRecorder extends Serializable {

    void prepare();

    void setLogger(FileLogger logger);

    boolean record(String user, int size, String urls, String values);

    void stop();
}
