package com.persist.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * ���򵥵���producer
 * 
 * @author Administrator
 *
 */
public class KafkaNewProducer {
	public org.apache.kafka.clients.producer.KafkaProducer<String, String> producer = null;

	/**
	 * ��ʼ�����ڷ��͵�KafkaProducer����
	 * 
	 * @param brokerList
	 *            kafka�������ڵ��ַ
	 */
	public KafkaNewProducer(String brokerList) {
		Properties pro = new Properties();
		pro.put("bootstrap.servers", brokerList);
		pro.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		pro.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		this.producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(pro);
	}

	/**
	 * ��ָ����topic������Ϣ
	 * 
	 * @param topic
	 * @param message
	 * @throws Exception
	 */
	public void send(String topic, String message) throws Exception {
		if (producer != null) {
			producer.send(new ProducerRecord<String, String>(topic, message));
			// System.out.println("Send:" + message);
		} else {
			throw new Exception("Producer is null!");
		}
	}

	/**
	 * �ر�producer������
	 */
	public void close() {
		if (producer != null)
			producer.close();
	}

}