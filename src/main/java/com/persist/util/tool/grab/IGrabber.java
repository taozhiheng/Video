package com.persist.util.tool.grab;

import java.io.Serializable;

/**
 * Created by taozhiheng on 16-7-15.
 *
 */
public interface IGrabber extends Serializable {

    Process grab(String host, int port, String password, String url, String dst, String sendTopic, String brokerList);

}
