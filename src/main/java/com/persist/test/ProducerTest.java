package com.persist.test;

import com.google.gson.Gson;
import com.persist.bean.analysis.PictureKey;
import com.persist.kafka.KafkaNewProducer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Created by taozhiheng on 16-8-11.
 *
 */
public class ProducerTest {

    public static void main(String[] args) throws Exception
    {
        KafkaNewProducer producer = new KafkaNewProducer("zk01:9092,zk02:9092,zk03:9092");
        int n = Integer.parseInt(args[1]);
        String topic = "test";
        PictureKey key = new PictureKey(args[0], null, null);
        Gson gson = new Gson();
        String msg;
        Callback callback = new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if(e != null)
                    e.printStackTrace();
                System.out.println("The offset of the record is: " + recordMetadata.offset());
            }
        };
        for(int i = 0; i < n; i++)
        {
            try {
                key.video_id = "ProducerTest "+i;
                key.time_stamp = String.valueOf(System.currentTimeMillis());
                msg = gson.toJson(key);
                producer.send(topic, msg, callback);
                System.out.println("send msg "+i);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        System.out.println("Sleep for 3s for test");
        Thread.sleep(3000);
        producer.close();
        System.out.println("send finish");
    }

}
