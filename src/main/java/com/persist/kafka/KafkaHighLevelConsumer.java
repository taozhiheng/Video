package com.persist.kafka;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;

/**
 * ʹ�ø߼�consumer�������ṩoffset�ľ�ȷ���ƣ���consumerȷ����ɶ���Ϣ�Ĵ�����ֶ��ύoffset��ȷ������ʱ�����ض���Ҫ�Ĳ���
 * �ṩ��3�ֻ�ȡ��Ϣ�ķ���
 * ͬʱ֧�ֱȽϵ�ǰ��ȡ����Ϣ�Ƿ������������µ�offset��������������ֱ������������Ϣ�������µ���Ϣ��ʼ��Ҫʹ�øù��ܣ����ֶ�����compareOffset(long)����
 * ��������ÿ�ζ�ȡ��Ϣ�����ø÷�������Ϊ�÷���ִ��һ�ε�ʱ����ܽ����ڶ�ȡһ����Ϣ��ʱ��
 * (�������⣺consumer����ʹ�ú�û�е���close()�ͷ���Դ����δ�������⣬��Ҫ����)
 * 
 * @author Administrator
 *
 */
public class KafkaHighLevelConsumer {
	private String topic;
	private String brokerList;
	private String groupId;
	private String clientId;
	private KafkaConsumer<String, String> consumer = null;

	/**
	 * 
	 * @param kafkaBrokers
	 *            kafka�������ڵ㣬��ʽΪ"hostname:port,hostname:port,..."
	 * @param groupId
	 *            �����߷������ƣ�ÿ������ֻ��һ�������������Ѹ�topic
	 * @param topic
	 *            topic����
	 */
	public KafkaHighLevelConsumer(String kafkaBrokers, String topic, String groupId, String clientId) {
		this.brokerList = kafkaBrokers;
		this.groupId = groupId;
		this.topic = topic;
		this.clientId = clientId;
		consumer = CreateConsumer();
		consumer.subscribe(Arrays.asList(topic));
	}

	/**
	 * ��ʼ��consumer
	 * 
	 * @return
	 */
	private KafkaConsumer<String, String> CreateConsumer() {
		Properties pro = new Properties();
		pro.put("bootstrap.servers", brokerList);
		pro.put("group.id", groupId);
		pro.put("client.id", clientId);
		pro.put("enable.auto.commit", "false");
		pro.put("auto.commit.interval.ms", "1000");
		pro.put("session.timeout.ms", "30000");
		pro.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		pro.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		return new KafkaConsumer<String, String>(pro);
	}

	/**
	 * ͨ����ѭ�����ϻ�ȡ��Ϣ�������̳��˽ӿ�MethodInterface�����ڴ�������
	 * 
	 * @param minterface
	 *            ʵ���Լ������ݵĴ���������
	 */
	public void getMessage(MethodInterface minterface) {
		final int minBatchSize = 0;// �����ڽ���Ϣѹ�����ͣ�����IO����������Ч��
		List<ConsumerRecord<String, String>> buffer = new ArrayList<ConsumerRecord<String, String>>();
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(0);// ������������ò�������Ϊ0�����Լ�ʹ����Ŀǰ�ɶ���������Ϣ������Ϊ����ֵ���������ٶȺ����Ҷ�������ϢΪ��
			for (ConsumerRecord<String, String> record : records) {
				buffer.add(record);
			}
			// ����Ϣ���۵�һ���������ÿ����Ϣ���д���
			if (buffer.size() > minBatchSize) {
				for (ConsumerRecord<String, String> element : buffer) {
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
	 * ��΢�ı书�ܰ棬��ʹ����ѭ��������ȡ����Ϣ��������ָ����������д������أ�����minBatchSize�������0
	 * 
	 * @param minterface
	 *            ʵ�ִ��������Զ�����
	 * @param minBatchSize
	 *            ÿ������ȡ��������Ϣ
	 */
	public void getMessage(MethodInterface minterface, int minBatchSize) {
		List<ConsumerRecord<String, String>> buffer = new ArrayList<ConsumerRecord<String, String>>();
		// ֱ����Ϣ��������ָ��������Ϊֹ�����ϻ�ȡ��Ϣ
		while (buffer.size() < minBatchSize) {
			ConsumerRecords<String, String> records = consumer.poll(0);// ������������ò�������Ϊ0�����Լ�ʹ����Ŀǰ�ɶ���������Ϣ������Ϊ����ֵ���������ٶȺ����Ҷ�������ϢΪ��
			for (ConsumerRecord<String, String> record : records) {
				buffer.add(record);
			}
		}
		for (ConsumerRecord<String, String> element : buffer) {
			minterface.dealWithData(element.value());// ������Ӧʵ��MethodInterface�ӿ�
		}
		consumer.commitSync();// �ύȷ����Ϣ������ʽ�ĸ���offset
	}

	/**
	 * ��һ���Ķ����ܰ棬�����Զ����д�����ύȷ�ϣ�������Ϣ�����������Ҫ�ֶ�����commit()�ύȷ����Ϣ��������ض�ͬ������Ϣ�������������0
	 * 
	 * @param minBatchSize
	 *            ÿ������ȡ��������Ϣ
	 * @return
	 */
	public List<String> getMessage(int minBatchSize) {
		List<ConsumerRecord<String, String>> buffer = new ArrayList<ConsumerRecord<String, String>>();
		List<String> values = new ArrayList<String>();
		// ֱ����Ϣ��������ָ��������Ϊֹ�����ϻ�ȡ��Ϣ
		while (buffer.size() < minBatchSize) {
			ConsumerRecords<String, String> records = consumer.poll(0);// ������������ò�������Ϊ0�����Լ�ʹ����Ŀǰ�ɶ���������Ϣ������Ϊ����ֵ���������ٶȺ����Ҷ�������ϢΪ��
			for (ConsumerRecord<String, String> record : records) {
				buffer.add(record);
			}
		}
		for (ConsumerRecord<String, String> element : buffer) {
			values.add(element.value());
		}
		return values;
	}

	/**
	 * �ֶ����ø÷������ύ����ȡ��offset
	 */
	public void commit() {
		consumer.commitSync();
	}

	/**
	 * ��ȡconsumer���ȷ���ϴ���offset
	 * 
	 * @return
	 */
	private long getCommittedOffset() {
		return consumer.committed(new TopicPartition(topic, 0)).offset();
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
		 * KafkaOffset.getLatestOffset(topic, groupId, port, brokerIP); if
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
			long offset = KafkaOffset.getLatestOffset(topic, groupId, port, seed);
			if (offset != -1)
				latestOffset = offset;
		}
		if (latestOffset == -1)
			return;
		else if (latestOffset - committedOffset >= difference)
			setOffset(latestOffset);
	}

	public static void main(String[] args)
	{
		KafkaHighLevelConsumer kc=new KafkaHighLevelConsumer(args[0],args[1],args[2], args[3]);
		while(true)
		{
			List<String> m=kc.getMessage(1);
			for(String e:m)
			{
				System.out.println(e);
			}
		}
	}
}

/*
 * class Deal implements MethodInterface { public Deal() {
 * 
 * }
 * 
 * public void dealWithData(String value) { System.out.println(value); } }
 */

class KafkaOffset {

	/**
	 * ��ȡ����offset
	 * 
	 * @param consumer
	 *            consumerʵ��
	 * @param topic
	 *            topic����
	 * @param partition
	 *            ������
	 * @param whichTime
	 *            ʹ��OffsetRequest.EarliestTime()��OffsetRequest.LatestTime()
	 * @param clientName
	 *            consumer�������ƣ�����Ҫ��ѯ�ķ������Ʊ�����ͬ
	 * @return
	 */
	private static long getLastOffset(SimpleConsumer consumer, String topic, int partition, long whichTime,
			String clientName) {
		TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
		Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
		OffsetRequest request = new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
		OffsetResponse response = consumer.getOffsetsBefore(request);

		if (response.hasError()) {
			System.out.println("Error getting Data Offset from brokers. Reason:"
					+ ErrorMapping.exceptionNameFor(response.errorCode(topic, partition)));
			return -1;
		}
		long[] offsets = response.offsets(topic, partition);
		return offsets[0];
	}

	/**
	 * ��ȡָ��topic��leader�ڵ㣨��δ���Լ�Ⱥ������������õ��ڵ㣩
	 * 
	 * @param a_seedBrokers
	 *            kafka�������ڵ�
	 * @param a_port
	 *            �˿ں�
	 * @param a_topic
	 *            topic����
	 * @return
	 */
	private static TreeMap<Integer, PartitionMetadata> findLeader(List<String> a_seedBrokers, int a_port,
			String a_topic) {
		TreeMap<Integer, PartitionMetadata> map = new TreeMap<Integer, PartitionMetadata>();
		for (String seed : a_seedBrokers) {
			SimpleConsumer consumer = null;
			consumer = new SimpleConsumer(seed, a_port, 100000, 64 * 1024, "leaderLookup" + new Date().getTime());
			List<String> topics = Collections.singletonList(a_topic);
			TopicMetadataRequest req = new TopicMetadataRequest(topics);
			TopicMetadataResponse resp = consumer.send(req);

			List<TopicMetadata> metaData = resp.topicsMetadata();
			for (TopicMetadata item : metaData) {
				for (PartitionMetadata part : item.partitionsMetadata()) {
					map.put(part.partitionId(), part);
				}
			}
			if (consumer != null)
				consumer.close();
		}
		return map;
	}

	/**
	 * ��ȡָ��topic������offset
	 * 
	 * @param topic
	 * @param port
	 * @param seed
	 *            kafka�������ڵ��ַ����δ���Լ�Ⱥ�����
	 * @return -1��ʾ���󣬷��򷵻�ָ��topic������
	 */
	public static long getLatestOffset(String topic, String clientName, int port, String... seed) {
		List<String> seeds = new ArrayList<String>();
		for (String item : seed) {
			seeds.add(item);
		}
		TreeMap<Integer, PartitionMetadata> metadatas = findLeader(seeds, port, topic);

		for (Entry<Integer, PartitionMetadata> entry : metadatas.entrySet()) {
			int partition = entry.getKey();
			String leadBroker = entry.getValue().leader().host();
			SimpleConsumer consumer = new SimpleConsumer(leadBroker, port, 100000, 64 * 1024, clientName);
			long readOffset = getLastOffset(consumer, topic, partition, kafka.api.OffsetRequest.LatestTime(),
					clientName);
			// System.out.println("read offset:" + readOffset);
			if (consumer != null)
				consumer.close();
			return readOffset;
		}
		return -1;
	}
}