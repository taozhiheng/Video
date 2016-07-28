package com.persist.util.tool.analysis;

import com.persist.bean.analysis.PictureResult;

import java.io.Serializable;

/**
 * Created by taozhiheng on 16-7-13.
 *
 */
public interface IPictureRecorder extends Serializable{

    void prepare();

    boolean recordResult(PictureResult result);

    void stop();
}
