package com.persist.util.tool.analysis;

import com.persist.bean.analysis.PictureResult;
import java.io.Serializable;

/**
 * Created by zhiheng on 2016/7/5.
 *
 */
public interface IPictureNotifier extends Serializable{

    void prepare();

    boolean notifyResult(PictureResult result);

    void stop();

}
