package com.persist.bean.analysis;

import java.io.Serializable;

/**
 * Created by zhiheng on 2016/7/4.
 *
 * Picture base info and result
 *
 */
public class PictureResult implements Serializable{
    //the description of the picture
    public PictureKey description;
    //the deal result of the picture,whether the picture is permit
    public boolean ok;
    public float percent;

    public PictureResult(PictureKey description, boolean ok, float percent)
    {
        this.description = description;
        this.ok = ok;
        this.percent = percent;
    }

}
