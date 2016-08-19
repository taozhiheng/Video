package com.persist.kafka;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * ���򵥵���producer
 * 
 * @author ZhihengTao
 *
 */
public class KafkaNewProducer {

	private KafkaProducer<String, String> producer;

	/**
	 * Note:
	 * set forceFlush=true may be dangerous.
	 *
	 * The problem may happen like:
	 * org.apache.kafka.common.errors.InvalidTimestampException:
	 * The timestamp of the message is out of acceptable range.
	 * */
	private boolean forceFlush;

	public KafkaNewProducer(String brokerList) {
		this(brokerList, false);
	}

	public KafkaNewProducer(String brokerList, boolean forceFlush)
	{
		Properties properties = new Properties();
		properties.put("bootstrap.servers", brokerList);
		properties.put("acks", "all");
		properties.put("linger.ms", 5);
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		this.producer = new KafkaProducer<String, String>(properties);
		this.forceFlush = forceFlush;
	}

	public void setForceFlush(boolean forceFlush)
	{
		this.forceFlush = forceFlush;
	}

	public boolean getForceFlush()
	{
		return forceFlush;
	}

	public boolean send(String topic, String message)
	{
		return send(topic, message, null);
	}


	public boolean send(String topic, String message, Callback callback)
	{
		if(producer == null)
			return false;
		producer.send(new ProducerRecord<String, String>(topic, message), callback);
		//may be dangerous to force flushing
		if(forceFlush)
			producer.flush();
		return true;
	}

	public boolean flush()
	{
		if(producer == null)
			return false;
		producer.flush();
		return true;
	}


	public void close() {
		if (producer != null) {
			producer.close();
			producer = null;
		}
	}

}