package com.persist.kafka;

import java.io.Serializable;
import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * ����producer��֧�ִ����Զ���ĸ�������󣬴���Ķ�����Ҫʵ��Serializable�ӿ�
 * 
 * @author Administrator
 *
 */
public class KafkaObjectProducer {
	public org.apache.kafka.clients.producer.KafkaProducer<String, Serializable> producer = null;

	/**
	 * ��ʼ�����ڷ��͵�KafkaProducer���󣬲���Ϊkafka�������ڵ㣬��һ���Ǽ�Ⱥ�����нڵ�ĵ�ַ����Ϊ�˷�ֹ����ָ���ڵ���ϵ����޷����ּ�Ⱥ����þ�����ָ���ڵ��ַ
	 * 
	 * @param brokerList
	 *            kafka�������ڵ��ַ
	 */
	public KafkaObjectProducer(String brokerList) {
		Properties pro = new Properties();
		pro.put("bootstrap.servers", brokerList);
		pro.put("key.serializer", "com.neptune.KafkaSerializer");
		pro.put("value.serializer", "com.neptune.KafkaSerializer");
		this.producer = new org.apache.kafka.clients.producer.KafkaProducer<String, Serializable>(pro);
	}

	/**
	 * ���͸�����Ķ���
	 * 
	 * @param topic
	 * @param message
	 * @throws Exception
	 */
	public void send(String topic, Serializable message) throws Exception {
		if (producer != null) {
			producer.send(new ProducerRecord<String, Serializable>(topic, message));
			// System.out.println("Send:" + message);
		} else {
			System.out.println("Producer initializing failed!");
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