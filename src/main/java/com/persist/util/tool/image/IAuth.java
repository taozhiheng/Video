package com.persist.util.tool.image;

import java.io.Serializable;

/**
 * Created by taozhiheng on 16-10-13.
 *
 */
public interface IAuth extends Serializable{

    void prepare();

    boolean auth(String username, String password);

    void stop();

}
