package com.persist.util.tool.analysis;

import com.persist.bean.analysis.PictureKey;
import com.persist.bean.analysis.PictureResult;
import java.io.Serializable;
import java.util.List;

/**
 * Created by zhiheng on 2016/7/5.
 *
 */
public interface IPictureCalculator extends Serializable{

    void prepare();

    void cleanup();

    List<PictureResult> calculateImage(PictureKey key);
}
