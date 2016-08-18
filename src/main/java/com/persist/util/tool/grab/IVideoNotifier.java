package com.persist.util.tool.grab;

import java.io.Serializable;

/**
 * Created by taozhiheng on 16-7-20.
 *
 */
public interface IVideoNotifier extends Serializable{

    void prepare();

    void notify(String msg);

    void stop();
}
