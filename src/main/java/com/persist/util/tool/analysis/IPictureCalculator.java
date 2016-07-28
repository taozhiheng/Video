package com.persist.util.tool.analysis;

import com.persist.bean.analysis.PictureKey;
import com.persist.bean.analysis.PictureResult;

import java.io.Serializable;

/**
 * Created by zhiheng on 2016/7/5.
 *
 */
public interface IPictureCalculator extends Serializable{

    void prepare();

    PictureResult calculateImage(PictureKey entity);
}
