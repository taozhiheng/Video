package com.persist.test;

import java.util.Iterator;
import java.util.Map;

/**
 * Created by taozhiheng on 16-8-1.
 *
 */
public class PathTest {

    public static void main(String[] args)
    {
        Map<String, String> map = System.getenv();
        for(Iterator<String> itr = map.keySet().iterator();itr.hasNext();){
            String key = itr.next();
            System.out.println(key + "=" + map.get(key));
        }
    }

}
