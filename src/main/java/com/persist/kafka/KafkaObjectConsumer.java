package com.persist.kafka;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * ��KafkaHighLevelConsumer�Ļ����Ͻ��иĽ���֧�ֽ����Զ���ĸ�������󣬸������ʵ��Serializable�ӿڣ��������÷���KafkaHighLevelConsumer��ͬ
 * (�������⣺consumer����ʹ�ú�û�е���close()�ͷ���Դ����δ�������⣬��Ҫ����)
 * 
 * @author Administrator
 *
 */
public class KafkaObjectConsumer {
	private String topic;
	private String brokerList;
	private String groupID;
	private KafkaConsumer<Serializable, Serializable> consumer = null;

	/**
	 * 
	 * @param kafkaBrokers
	 *            kafka�������ڵ㣬��ʽΪ"hostname:port,hostname:port,..."
	 * @param consumerGroup
	 *            �����߷������ƣ�ÿ������ֻ��һ�������������Ѹ�topic
	 * @param topic
	 *            topic����
	 */
	public KafkaObjectConsumer(String kafkaBrokers, String consumerGroup, String topic) {
		brokerList = kafkaBrokers;
		groupID = consumerGroup;
		this.topic = topic;
		consumer = CreateConsumer();
		consumer.subscribe(Arrays.asList(topic));
	}

	/**
	 * ��ʼ��consumer
	 * 
	 * @return
	 */
	private KafkaConsumer<Serializable, Serializable> CreateConsumer() {
		Properties pro = new Properties();
		pro.put("bootstrap.servers", brokerList);
		pro.put("group.id", groupID);
		pro.put("enable.auto.commit", "false");
		pro.put("auto.commit.interval.ms", "1000");
		pro.put("session.timeout.ms", "30000");
		pro.put("key.deserializer", "com.neptune.KafkaDeserializer");
		pro.put("value.deserializer", "com.neptune.KafkaDeserializer");
		return new KafkaConsumer<Serializable, Serializable>(pro);
	}

	/**
	 * 
	 * @param minterface
	 */
	public void getMessage(MethodInterface minterface) {
		final int minBatchSize = 0;// �����ڽ���Ϣѹ�����ͣ�����IO����������Ч��
		List<ConsumerRecord<Serializable, Serializable>> buffer = new ArrayList<ConsumerRecord<Serializable, Serializable>>();
		while (true) {
			ConsumerRecords<Serializable, Serializable> records = consumer.poll(0);// ������������ò�������Ϊ0�����Լ�ʹ����Ŀǰ�ɶ���������Ϣ������Ϊ����ֵ���������ٶȺ����Ҷ�������ϢΪ��
			for (ConsumerRecord<Serializable, Serializable> record : records) {
				buffer.add(record);
			}
			// ����Ϣ���۵�һ���������ÿ����Ϣ���д���
			if (buffer.size() > minBatchSize) {
				for (ConsumerRecord<Serializable, Serializable> element : buffer) {
					minterface.dealWithData(element.value());// ������Ӧʵ��MethodInterface�ӿ�
				}
				consumer.commitSync();// �ύȷ����Ϣ������ʽ�ĸ���offset
				// TODO ������䣬��ɾ��
				// System.out.println(getCommittedOffset());
				buffer.clear();
			}
			// System.out.println("end of while");
		}
	}

	/**
	 * 
	 * @param minterface
	 * @param minBatchSize
	 */
	public void getMessage(MethodInterface minterface, int minBatchSize) {
		List<ConsumerRecord<Serializable, Serializable>> buffer = new ArrayList<ConsumerRecord<Serializable, Serializable>>();
		// ֱ����Ϣ��������ָ��������Ϊֹ�����ϻ�ȡ��Ϣ
		while (buffer.size() < minBatchSize) {
			ConsumerRecords<Serializable, Serializable> records = consumer.poll(0);// ������������ò�������Ϊ0�����Լ�ʹ����Ŀǰ�ɶ���������Ϣ������Ϊ����ֵ���������ٶȺ����Ҷ�������ϢΪ��
			for (ConsumerRecord<Serializable, Serializable> record : records) {
				buffer.add(record);
			}
		}
		for (ConsumerRecord<Serializable, Serializable> element : buffer) {
			minterface.dealWithData(element.value());// ������Ӧʵ��MethodInterface�ӿ�
		}
		consumer.commitSync();// �ύȷ����Ϣ������ʽ�ĸ���offset
	}

	/**
	 * 
	 * @param minBatchSize
	 * @return
	 */
	public List<Serializable> getMessage(int minBatchSize) {
		List<ConsumerRecord<Serializable, Serializable>> buffer = new ArrayList<ConsumerRecord<Serializable, Serializable>>();
		List<Serializable> values = new ArrayList<Serializable>();
		// ֱ����Ϣ��������ָ��������Ϊֹ�����ϻ�ȡ��Ϣ
		while (buffer.size() < minBatchSize) {
			ConsumerRecords<Serializable, Serializable> records = consumer.poll(0);// ������������ò�������Ϊ0�����Լ�ʹ����Ŀǰ�ɶ���������Ϣ������Ϊ����ֵ���������ٶȺ����Ҷ�������ϢΪ��
			for (ConsumerRecord<Serializable, Serializable> record : records) {
				buffer.add(record);
			}
			System.out.println(buffer.isEmpty());
		}
		for (ConsumerRecord<Serializable, Serializable> element : buffer) {
			values.add(element.value());
		}
		return values;
	}

	/**
	 * 
	 */
	public void commit() {
		consumer.commitSync();
	}

	/**
	 * �ϴ��Լ��趨��offset������ʹ�´ζ�ȡ�Ӹ�offset����ʼ�����ܹ���С�ڵ�ǰoffset����������poll��ʱ��������ָ��offset�ѱ�ɾ��
	 * 
	 * @param offset
	 */
	private void setOffset(long offset) {
		Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<TopicPartition, OffsetAndMetadata>();
		offsets.put(new TopicPartition(topic, 0), new OffsetAndMetadata(offset));
		consumer.commitSync(offsets);
	}

	/**
	 * �Ƚϵ�ǰoffset�����µ�offset������ֵ����ָ���Ĳ�����������µ�offset����ʼ��ȡ
	 * 
	 * @param difference
	 *            ��ǰoffset������offset֮�������ֵ������������֮����ڸ�ֵ���򽫴�����offset����ʼ��ȡ
	 */
	public void compareOffsets(long difference) {
		/*
		 * long committedOffset = getCommittedOffset(); long latestOffset =
		 * KafkaOffset.getLatestOffset(topic, groupID, port, brokerIP); if
		 * (latestOffset == -1) return; else { if (latestOffset -
		 * committedOffset >= difference) setOffset(latestOffset); }
		 */
		long committedOffset = getCommittedOffset();
		long latestOffset = -1;
		// �����Զ��Ÿ����Ķ�ɵ�ַ
		String[] brokers = brokerList.split(",");
		for (String broker : brokers) {
			// �����ַ�е�IP�Ͷ˿ں�
			String[] ids = broker.split(":");
			String seed = ids[0];
			int port = Integer.valueOf(ids[1]);
			long offset = KafkaOffset.getLatestOffset(topic, groupID, port, seed);
			if (offset != -1)
				latestOffset = offset;
		}
		if (latestOffset == -1)
			return;
		else if (latestOffset - committedOffset >= difference)
			setOffset(latestOffset);
	}

	/**
	 * ��ȡconsumer���ȷ���ϴ���offset
	 * 
	 * @return
	 */
	private long getCommittedOffset() {
		return consumer.committed(new TopicPartition(topic, 0)).offset();
	}

	/*
	 * public static void main(String args[]) throws Exception { O o=new O();
	 * o.start(); KafkaObjectConsumer c = new
	 * KafkaObjectConsumer("192.168.0.169:9092", "groupObject",
	 * "dont-delete-this-topic"); c.getMessage(new Deal()); }
	 */
}

/*
 * class TestData implements Serializable { private static final long
 * serialVersionUID = 1L; int i = 10; String s = "testing"; double d =
 * 3.1415926; }
 */

/*
 * class Deal implements MethodInterface {
 * 
 * @Override public void dealWithData(Object value) { // TODO Auto-generated
 * method stub TestData d = (TestData) value; System.out.println("int:" + d.i +
 * ",string:" + d.s + ",double:" + d.d); }
 * 
 * }
 */

/*
 * class O extends Thread { public void run() { TestData data = new TestData();
 * KafkaObjectProducer p = new KafkaObjectProducer("192.168.0.169:9092");
 * while(true) { try { p.send("dont-delete-this-topic", data); } catch
 * (Exception e) { // TODO Auto-generated catch block e.printStackTrace(); } } }
 * }
 */