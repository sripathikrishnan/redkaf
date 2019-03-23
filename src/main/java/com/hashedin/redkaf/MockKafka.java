package com.hashedin.redkaf;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;

import com.hashedin.redkaf.MyConsumer.MyTopicPartitionState;

public class MockKafka {
	
	private static final long MAX_RECORDS_PER_POLL = 10;
	private final String clusterId;
	private final Set<Node> nodes;
	private final Map<TopicPartition, List<ProducerRecord<byte[], byte[]>>> recordsByPartition;
	private final Map<String, List<TopicPartition>> partitionsByTopic;
	private final Set<String> internalTopics;
	
	MockKafka(String clusterId, Set<Node> nodes) {
		this.clusterId = clusterId;
		this.nodes = nodes;
		this.recordsByPartition = new HashMap<>();
		this.partitionsByTopic = new HashMap<>();
		this.internalTopics = new HashSet<>();
	}

	public ConsumerRecords<byte[], byte[]> poll(Set<TopicPartition> assignments, Map<TopicPartition, MyTopicPartitionState> partionStates, long timeout) {
		Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> toReturn = new HashMap<>();
		
		for(TopicPartition tp: assignments) {
			if(!recordsByPartition.containsKey(tp) || !partionStates.containsKey(tp)) {
				continue;
			}
			List<ProducerRecord<byte[], byte[]>> allRecords = recordsByPartition.get(tp);
			List<ConsumerRecord<byte[], byte[]>> recordsToDeliver = new ArrayList<>();
			
			MyTopicPartitionState partitionState = partionStates.get(tp);
			
			int offset = position(tp, partitionState);
			int i = offset;
			for(; i< offset + MAX_RECORDS_PER_POLL; i++) {
				if (i >= allRecords.size()) {
					break;
				}
				ProducerRecord<byte[], byte[]> record = allRecords.get(i);
				if(record.partition() == null) {
					System.out.println("Null record!");
				}
				recordsToDeliver.add(toConsumerRecord(record, i));
			}
			partitionState.setOffset((long)i);
			
			if(!recordsToDeliver.isEmpty()) {
				toReturn.put(tp, recordsToDeliver);
			}
		}
		
		return new ConsumerRecords<>(toReturn);
	}
	
	public int position(TopicPartition tp, MyTopicPartitionState partitionState) {
		List<ProducerRecord<byte[], byte[]>> allRecords = recordsByPartition.get(tp);
		Long offsetL = partitionState.getOffset();
		if (offsetL < 0) {
			if (partitionState.getOffsetResetStrategy() == OffsetResetStrategy.EARLIEST) {
				offsetL = 0L;
			}
			else if (partitionState.getOffsetResetStrategy() == OffsetResetStrategy.LATEST) {
				offsetL = (long)allRecords.size();
			}
			else {
				throw new NoOffsetForPartitionException(Collections.singleton(tp));
			}
			
		}
		if (offsetL > Integer.MAX_VALUE) {
			throw new RuntimeException("Did not expect more than " + Integer.MAX_VALUE + " records!");
		}
		int offset = offsetL.intValue();
		return offset;
	}
	
	private ConsumerRecord<byte[], byte[]> toConsumerRecord(ProducerRecord<byte[], byte[]> record, long offset) {		
		return new ConsumerRecord<byte[], byte[]>(record.topic(), record.partition(), offset, 
				record.timestamp(), TimestampType.CREATE_TIME,
				0L,
				record.key().length, record.value().length,
				record.key(), record.value());
	}

	public void createTopics(Collection<NewTopic> newTopics, CreateTopicsOptions options) {
		for(NewTopic topic : newTopics) {
			if(determineIfInternalTopic(topic.name())) {
				internalTopics.add(topic.name());
			}
			List<TopicPartition> partitions = new ArrayList<>();
			for(int i=0; i<topic.numPartitions(); i++) {
				TopicPartition tp = new TopicPartition(topic.name(), i); 
				partitions.add(tp);
				recordsByPartition.put(tp, new ArrayList<>());
			}
			partitionsByTopic.put(topic.name(), partitions);
		}
	}

	private static boolean determineIfInternalTopic(String topicName) {
		/*
		 * Sri: Don't know how Kafka decides if a topic is internal or not
		 * We will use a hack for now. If the topic has  
		 */
		if(topicName.contains("KSTREAM") || topicName.contains("repartition")) {
			return true;
		}
		return false;
	}
	
	public boolean isInternalTopic(String topicName) {
		return internalTopics.contains(topicName);
	}
	
	public Collection<String> getTopicsMatching(Pattern pattern) {
		Set<String> topics = new HashSet<>();
		for(String topic : partitionsByTopic.keySet()) {
			if(pattern.matcher(topic).matches()) {
				topics.add(topic);
			}
		}
		return topics;
	}

	private Collection<PartitionInfo> allPartitions() {
		Set<PartitionInfo> allPartitions = new HashSet<>();
		for(Map.Entry<String, List<TopicPartition>> entry: partitionsByTopic.entrySet()) {
			for(TopicPartition tp : entry.getValue()) {
				allPartitions.add(toPartitionInfo(tp));
			}
		}
		return allPartitions;
	}
	
	PartitionInfo toPartitionInfo(TopicPartition tp) {
		return new PartitionInfo(tp.topic(), tp.partition(), leaderNode(), new Node[0], new Node[0]);
	}

	public Node leaderNode() {
		// For now, we don't really care who is the leader for a given partition
		// We just respect the interface and return a Node
		return nodes.iterator().next();
	}

	public Cluster getClusterMetadata() {
		return new Cluster(clusterId, nodes, allPartitions(), Collections.emptySet(), Collections.unmodifiableSet(internalTopics));
	}

	public RecordMetadata send(ProducerRecord<byte[], byte[]> record, int partition) {
		String topic = record.topic();
		TopicPartition tp = new TopicPartition(topic, partition);
		
		if(record.partition() == null) {
			record = new ProducerRecord<byte[], byte[]>(topic, partition, record.timestamp(), record.key(), record.value());
		}
		List<ProducerRecord<byte[], byte[]>> records = recordsByPartition.get(tp);
		records.add(record);
		long offset = records.size();
		final RecordMetadata meta = new RecordMetadata(tp, 0, offset, System.currentTimeMillis(), 0L, 
				record.key().length, record.value().length);
		return meta;
	}

	public List<PartitionInfo> partitionsFor(String topic) {
		List<TopicPartition> partitions = partitionsByTopic.get(topic);
		List<PartitionInfo> toReturn = new ArrayList<>();
		for(TopicPartition tp: partitions) {
			toReturn.add(toPartitionInfo(tp));
		}
		return toReturn;
	}

	public List<TopicPartition> getTopicPartitions(String topicName) {
		List<TopicPartition> partitions = partitionsByTopic.get(topicName);
		if (partitions == null) {
			partitions = Collections.emptyList();
		}
		return partitions;
	}

	public Long getEndOffset(TopicPartition partition) {
		if(!recordsByPartition.containsKey(partition)) {
			return 0L;
		}
		return (long)recordsByPartition.get(partition).size();
	}

	public Map<String, List<PartitionInfo>> listTopics() {
		Map<String, List<PartitionInfo>> topics = new HashMap<>();
		
		for(TopicPartition tp: recordsByPartition.keySet()) {
			List<PartitionInfo> partitions = topics.computeIfAbsent(tp.topic(), key -> new ArrayList<PartitionInfo>());
			partitions.add(toPartitionInfo(tp));
		}
		
		return Collections.unmodifiableMap(topics);
	}
}
