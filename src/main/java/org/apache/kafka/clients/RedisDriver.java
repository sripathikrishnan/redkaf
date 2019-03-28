package org.apache.kafka.clients;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MyTopicPartitionState;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;

import com.hashedin.redkaf.KafkaRedisOffsetMapper;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.commands.ProtocolCommand;

public class RedisDriver implements AutoCloseable {

	/*
	 * The global redis set in which all topic names are stored
	 * We force metadata to be in the same redis cluster slot, so that we can use pipelining
	 */
	private static final String ALL_TOPICS_KEY = "__{redkaf}__all_topics";
	
	/*
	 * The redis hash for each topic.
	 * We force metadata to be in the same redis cluster slot, so that we can use pipelining
	 * 
	 * Example hash in redis: __redkaf__.topic.iot-data
	 */
	private static final String TOPIC_KEY_PREFIX = "__{redkaf}__.topic.";
	
	/*
	 * The redis stream for each topic partition
	 * Note that this intentionally does not have {}. 
	 * This ensures that redis cluster distributes the keys across multiple slots
	 * 
	 * Example stream in redis: __redkaf__.topic.iot-data.partition.1
	 */
	private static final String TOPIC_PARTITION_KEY_PREFIX = "__redkaf__.topic.";
	private static final String TOPIC_PARTITION_KEY_SUFFIX = ".partition.";
	
	/*
	 * A redis hash to store the committed offsets for each consumer
	 * The field name is a combination of (consumer group, consumer, topic partition)
	 * The value is a long representing the last commit for that consumer
	 */
	private static final String COMMITTED_OFFSETS_KEY = "__{redkaf}__committed_offsets";
	
	private static enum StreamCommand implements ProtocolCommand {
		XRANGE, XADD, XREAD, XREVRANGE;
		
		private final byte[] raw;

		StreamCommand() {
			raw = this.name().getBytes();
		}
		@Override
		public byte[] getRaw() {
			return raw;
		}
		
	}
	
	private static final String ASTERISK = "*";
	private static final String KEY = "__redkaf_k";
	private static final String VALUE = "__redkaf_v";
	private static final String TIMESTAMP = "__redkaf_t";
	private static final String FIELD_NUM_PARTITIONS = "numPartitions";
	private static final String FIELD_IS_INTERNAL_TOPIC = "isInternalTopic";
	
	private final JedisPool jedisPool;
	private final Node node;
	
	
	/**
	 * Create a driver from a Kafka Configuration object
	 * 
	 * The constructor assumes that instead of kafka brokers, the bootstrap servers points to redis server(s)
	 * @param config either ProducerConfig or ConsumerConfig object
	 */
	public RedisDriver(AbstractConfig config) {
		List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(
                config.getList(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG),
                config.getString(ProducerConfig.CLIENT_DNS_LOOKUP_CONFIG));
		
		if(addresses.size() > 1) {
			throw new IllegalArgumentException("Bootstrap servers must contain exactly one host:port for now");
		}
		
		InetSocketAddress address = addresses.get(0);
		jedisPool = new JedisPool(address.getHostString(), address.getPort());
		node = new Node(1, address.getHostString(), address.getPort());
	}
	
	/**
	 * Writes a record to Redis
	 * 
	 * This method internally calls XADD on a redis stream. The fields of the record are key, value and timestamp.
	 * 
	 * @param topicPartition the Kafka topic partition to which we must send the record. Internally, this is mapped to a redis stream
	 * @param headers - headers of the record. Currently ignored
	 * @param key - the key
	 * @param value - the value
	 * @param timestamp the timestamp
	 * @return RecordMetadata that contains the offset of the newly inserted record.
	 */
	public RecordMetadata send(TopicPartition topicPartition, Header[] headers, byte key[], byte value[], long timestamp) {
		final long checksum = 0l;
		final int serializedKeySize = key.length;
		final int serializedValueSize = value.length;
		
		try(Jedis jedis = jedisPool.getResource()) {
			String redisKey = getKeyForTopicPartition(topicPartition);
			jedis.getClient().sendCommand(StreamCommand.XADD, redisKey.getBytes(), ASTERISK.getBytes(), 
					KEY.getBytes(), key, VALUE.getBytes(), value, TIMESTAMP.getBytes(), longToBytes(timestamp));
			byte result [] = jedis.getClient().getBinaryBulkReply();
			String redisEntryId = new String(result);
			long offset = KafkaRedisOffsetMapper.toKafkaOffset(redisEntryId);
			
			RecordMetadata metadata = new RecordMetadata(topicPartition, 0L, offset, timestamp, 
					checksum, serializedKeySize, serializedValueSize);
			
			return metadata;
		}
	}
	
	/**
	 * Gets information about the Cluster
	 * 
	 * This method creates a minimal Kafka Cluster object from a Redis connection. 
	 * Specifically - 
	 * 1. For now, the cluster contains a single Node
	 * 2. The partitions do not have replicas  
	 * 3. All topics are accessible. In other words, unauthorizedTopics is empty.
	 * 
	 * @return the cluster object
	 */
	public Cluster getCluster() {
		try(Jedis jedis = jedisPool.getResource()) {
			Set<String> tempTopicNames = jedis.smembers(ALL_TOPICS_KEY);
			
			/* We need a deterministic order for pipeline to work*/
			List<String> topicNames = new ArrayList<>(tempTopicNames);
			Pipeline pipeline = jedis.pipelined();
			for(String topic : topicNames) {
				pipeline.hgetAll(getKeyForTopic(topic));
			}
			List<Object> allTopics = pipeline.syncAndReturnAll();
			return getCluster(topicNames, allTopics);
		}
	}

	private Cluster getCluster(List<String> topicNames, List<Object> topics) {
		String clusterId = getClusterId();
		Collection<Node> nodes = getClusterNodes();
		Set<String> unauthorizedTopics = Collections.emptySet();
		
		Set<PartitionInfo> partitions = new HashSet<>();
		Set<String> internalTopics = new HashSet<>();
		
		for(int i=0; i<topicNames.size(); i++) {
			@SuppressWarnings("unchecked")
			Map<String, String> topic = (Map<String, String>)topics.get(i);
			
			String topicName = topicNames.get(i);
			int numPartitions = Integer.parseInt(topic.get(FIELD_NUM_PARTITIONS));
			boolean isInternal = parseBoolean(topic.get(FIELD_IS_INTERNAL_TOPIC));
			if(isInternal) {
				internalTopics.add(topicName);
			}
			for(int j=0; j<numPartitions; j++) {
				partitions.add(toPartitionInfo(topicName, j));
			}
			
		}
		return new Cluster(clusterId, nodes, partitions, unauthorizedTopics, internalTopics);
	}

	/**
	 * Gets the partitions for a given topic
	 * 
	 * @param topic the name of the topic
	 * @return list of partitions for the given topic
	 */
	public List<PartitionInfo> partitionsFor(String topic) {
		List<PartitionInfo> partitions = new ArrayList<>();
		String topicKey = getKeyForTopic(topic);
		try(Jedis jedis = jedisPool.getResource()) {
			if(!jedis.exists(topicKey)) {
				return Collections.emptyList();
			}
			int numPartitions = Integer.parseInt(jedis.hget(topicKey, FIELD_NUM_PARTITIONS));
			for(int i=0; i<numPartitions; i++) {
				PartitionInfo partition = toPartitionInfo(topic, i);
				partitions.add(partition);
			}
			return partitions;
		}
	}
	

	public List<TopicListing> listTopics() {
		List<TopicListing> topics = new ArrayList<>();
		try(Jedis jedis = jedisPool.getResource()) {
			Set<String> topicNames = jedis.smembers(ALL_TOPICS_KEY);
			for(String topicName : topicNames) {
				topics.add(new TopicListing(topicName, isInternalTopic(topicName)));
			}
			return topics;
		}
	}
	
	public Map<String, List<PartitionInfo>> listTopicsWithPartitions() {
		Map<String, List<PartitionInfo>> topicsWithPartitions = new HashMap<>();
		try(Jedis jedis = jedisPool.getResource()) {
			List<String> topics = new ArrayList<>(jedis.smembers(ALL_TOPICS_KEY));
			
			Pipeline pipeline = jedis.pipelined();
			for(String topic : topics) {
				pipeline.hget(getKeyForTopic(topic), FIELD_NUM_PARTITIONS);
			}
			List<Object> partitionsByTopic = pipeline.syncAndReturnAll();
			
			for(int i=0; i<topics.size(); i++) {
				String topic = topics.get(i);
				int numPartitions = Integer.parseInt((String)partitionsByTopic.get(i));
				List<PartitionInfo> pinfos = new ArrayList<>();
				for(int j=0; j<numPartitions; j++) {
					pinfos.add(toPartitionInfo(topic, j));
				}
				topicsWithPartitions.put(topic, pinfos);
			}
		}
		return topicsWithPartitions;
	}
	
	public void createTopics(Collection<NewTopic> newTopics, CreateTopicsOptions options) {
		try(Jedis jedis = jedisPool.getResource()) {
			Pipeline pipeline = jedis.pipelined();
			
			for(NewTopic topic : newTopics) {
				jedis.sadd(ALL_TOPICS_KEY, topic.name());
				String topicKey = getKeyForTopic(topic.name());
				Map<String, String> topicMap = new HashMap<>();
				topicMap.put(FIELD_NUM_PARTITIONS, String.valueOf(topic.numPartitions()));
				topicMap.put(FIELD_IS_INTERNAL_TOPIC, String.valueOf(determineIfInternalTopic(topic.name())));
				pipeline.hset(topicKey, topicMap);
			}
			pipeline.syncAndReturnAll();
		}
	}

	public void deleteTopics(Collection<String> _topics) {
		/*We want a deterministic iteration order, so convert Collection to ArrayList*/
		List<String> topics = new ArrayList<>(_topics);
		try(Jedis jedis = jedisPool.getResource()) {
			Pipeline pipeline = jedis.pipelined();
			for(String topic : topics) {
				pipeline.hget(getKeyForTopic(topic), FIELD_NUM_PARTITIONS);
			}
			List<Object> partitionCounts = pipeline.syncAndReturnAll();
			
			/*
			 * TODO: there is a race condition because we are reading numPartitions and then deleting keys
			 * To fix - we can move this to lua or use a watch 
			 */
			pipeline = jedis.pipelined();
			for(int i=0; i<topics.size(); i++) {
				String topic = topics.get(i);
				String _numPartitions = (String)partitionCounts.get(i);
				int numPartitions = 0;
				if(_numPartitions != null && _numPartitions.length() > 0) {
					numPartitions = Integer.parseInt(_numPartitions);
				}
				pipeline.srem(ALL_TOPICS_KEY, topic);
				pipeline.del(getKeyForTopic(topic));
				for(int j=0; j<numPartitions; j++) {
					pipeline.del(getKeyForTopicPartition(new TopicPartition(topic, j)));
				}
			}
			pipeline.sync();
		}
	}

	public Node leaderNode() {
		return node;
	}
	
	public boolean isInternalTopic(String topicName) {
		return determineIfInternalTopic(topicName);
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
	
	private PartitionInfo toPartitionInfo(String topicName, int partition) {
		return new PartitionInfo(topicName, partition, node, new Node[0], new Node[0]);
	}

	private Collection<Node> getClusterNodes() {
		return Collections.singleton(node);
	}

	private String getClusterId() {
		return node.toString();
	}

	private static final String getKeyForTopic(String topic) {
		return TOPIC_KEY_PREFIX + topic;
	}
	
	private static final String getKeyForTopicPartition(TopicPartition topicPartition) {
		return TOPIC_PARTITION_KEY_PREFIX + topicPartition.topic() + TOPIC_PARTITION_KEY_SUFFIX + topicPartition.partition();
	}
	
	private static final TopicPartition getTopicPartitionFromKey(String key) {
		Pattern pattern = Pattern.compile(TOPIC_PARTITION_KEY_PREFIX + "(.*)" + TOPIC_PARTITION_KEY_SUFFIX + "(\\d*)");
		Matcher matcher = pattern.matcher(key);
		if(!matcher.matches()) {
			throw new IllegalStateException("Key does not match a TopicPartition - " + key);
		}
		
		String topic = matcher.group(1);
		int partition = Integer.parseInt(matcher.group(2));
		return new TopicPartition(topic, partition);
	}
	
	private static final String getFieldNameForCommittedOffsets(String groupId, String clientId, TopicPartition partition) {
		StringBuilder builder = new StringBuilder();
		builder.append(groupId).append("-").append(clientId).append("-")
				.append(partition.topic()).append("-").append(partition.partition());
		return builder.toString();
	}

	@Override
	public void close() throws Exception {
		jedisPool.close();
	}
	
	public Collection<String> getTopicsMatching(Pattern pattern) {
		Set<String> matchingTopics = new HashSet<>();
		try(Jedis jedis = jedisPool.getResource()) {
			Set<String> topics = jedis.smembers(ALL_TOPICS_KEY);
			for(String topic : topics) {
				if(pattern.matcher(topic).matches()) {
					matchingTopics.add(topic);
				}
			}
		}
		return matchingTopics;
	}

	@SuppressWarnings("unchecked")
	public <K,V> ConsumerRecords<K, V> poll(Set<TopicPartition> assignments,
			Map<TopicPartition, MyTopicPartitionState> partitionStates, long timeout,
			Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
		
		/*
		 * - Generate a list of redis keys from assignments, eliminating TopicPartition's that are paused
		 * - For each redis key, find the position from partitionStates
		 * - If position is null or < 0, determine position based on offsetResetStrategy
		 * - Convert the position to redis stream entry id using KafkaRedisOffsetMapper
		 * - Then construct XREAD command in the format 
		 * - XREAD COUNT 20 BLOCK <timeout> STREAMS key1 key2 key3 ... entryId1 entryId2 entryId3 ...
		 * - Convert each record into a ConsumerRecord
		 */
		long offset;
		List<String> streamKeys = new ArrayList<>();
		List<String> streamEntryIds = new ArrayList<>();
		for(TopicPartition partition : assignments) {
			/*step 1: If the partition is not paused, find the offset to poll from */
			MyTopicPartitionState state = partitionStates.get(partition);
			if (state != null) {
				if(state.isPaused()) {
					continue;
				}
				offset = state.getOffset();
				if(offset < 0) {
					if(OffsetResetStrategy.EARLIEST.equals(state.getOffsetResetStrategy())) {
						offset = getLogStartOffset(partition).offset();
					}
					else if (OffsetResetStrategy.LATEST.equals(state.getOffsetResetStrategy())) {
						offset = getHighWatermark(partition).offset();
					}
					else {
						throw new IllegalArgumentException("Partition " + partition + 
								" does not have an offset and does not have an OffsetResetStrategy");
					}
				}
			}
			else {
				/*TODO: do we use the defaultOffsetResetStrategy over here?*/
				throw new IllegalStateException("Missing state for TopicPartition " + partition);
			}
			
			/*step 2: Construct the stream keys and corresponding ids */
			streamKeys.add(getKeyForTopicPartition(partition));
			streamEntryIds.add(KafkaRedisOffsetMapper.toRedisEntryId(offset));
		}
		
		/* Step 3: Generate the command arguments */
		List<String> arguments = new ArrayList<>();
		arguments.addAll(Arrays.asList("count", "20", "block", String.valueOf(timeout), "streams"));
		arguments.addAll(streamKeys);
		arguments.addAll(streamEntryIds);
		
		/* Step 4: Execute the command */
		List<Object> response;
		try(Jedis jedis = jedisPool.getResource()) {
			jedis.getClient().sendCommand(StreamCommand.XREAD, arguments.toArray(new String[arguments.size()]));
			response = jedis.getClient().getObjectMultiBulkReply();
		}
		
		if (response == null || response.isEmpty()) {
			return null;
		}
		/* Step 5: Convert the response to a ConsumerRecord*/
		Map<TopicPartition, List<ConsumerRecord<K,V>>> toReturn = new HashMap<>();
		for(Object obj : response) {
			List<Object> recordsAndPartition = (List<Object>)obj;
			String redisKey = new String((byte[])recordsAndPartition.get(0));
			TopicPartition tp = getTopicPartitionFromKey(redisKey);
			
			List<Object> rawRecords = (List<Object>)recordsAndPartition.get(1);
			List<ConsumerRecord<K, V>> records = rawRecords.stream()
					.map(x -> deserialize(x, tp, keyDeserializer, valueDeserializer))
					.collect(Collectors.toList());
			
			toReturn.put(tp, records);
		}
		return new ConsumerRecords<K, V>(toReturn);
	}

	@SuppressWarnings("unchecked")
	private <K,V> ConsumerRecord<K,V> deserialize(Object obj, TopicPartition tp, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
		List<Object> rawRecord = (List<Object>)obj;
		String redisEntryId = new String((byte[])rawRecord.get(0));
		List<byte[]> kvpairs = (List<byte[]>)rawRecord.get(1);
		
		K key = null;
		V value = null;
		long timestamp = -1;
		int serializedKeySize = -1;
		int serializedValueSize = -1;
		
		for(int i=0; i<kvpairs.size(); i+=2) {
			String field = new String(kvpairs.get(i));
			byte data[] = kvpairs.get(i+1);
			if (KEY.equals(field)) {
				serializedKeySize = data.length;
				key = keyDeserializer.deserialize(tp.topic(), data);
			}
			else if (VALUE.equals(field)) {
				serializedValueSize = data.length;
				value = valueDeserializer.deserialize(tp.topic(), data);
			}
			else if (TIMESTAMP.equals(field)) {
				timestamp = bytesToLong(data);
			}
			else {
				// ignore unknown fields
			}
		}
		return new ConsumerRecord<K, V>(tp.topic(), tp.partition(), KafkaRedisOffsetMapper.toKafkaOffset(redisEntryId), 
				timestamp, TimestampType.CREATE_TIME, 0, serializedKeySize, serializedValueSize, key, value);
	}
	
	public void commit(String groupId, String clientId, TopicPartition tp, long offset) {
		String fieldName = getFieldNameForCommittedOffsets(groupId, clientId, tp);
		try(Jedis jedis = jedisPool.getResource()) {
			jedis.hset(COMMITTED_OFFSETS_KEY, Collections.singletonMap(fieldName, String.valueOf(offset)));
		}
	}

	public OffsetAndMetadata getCommitted(String groupId, String clientId, TopicPartition partition) {
		String fieldName = getFieldNameForCommittedOffsets(groupId, clientId, partition);
		try(Jedis jedis = jedisPool.getResource()) {
			String offsetStr = jedis.hget(COMMITTED_OFFSETS_KEY, fieldName);
			if(offsetStr != null && !offsetStr.isEmpty()) {
				long offset = Long.parseLong(offsetStr);
				return new OffsetAndMetadata(offset);
			}
			else {
				return null;
			}
		}
	}
	
	public OffsetAndMetadata getLogStartOffset(TopicPartition partition) {
		return getHighOrLowOffset(partition, false);
	}

	public OffsetAndMetadata getHighWatermark(TopicPartition partition) {
		return getHighOrLowOffset(partition, true);
	}

	private OffsetAndMetadata getHighOrLowOffset(TopicPartition partition, boolean reverse) {
		String redisKey = getKeyForTopicPartition(partition);
		try(Jedis jedis = jedisPool.getResource()) {
			if(reverse) {
				jedis.getClient().sendCommand(StreamCommand.XREVRANGE, 
						redisKey, "+", "-", "count", "1");
			}
			else {
				jedis.getClient().sendCommand(StreamCommand.XRANGE, 
						redisKey, "-", "+", "count", "1");
			}
			
			List<Object> response = jedis.getClient().getObjectMultiBulkReply();
			if(response.isEmpty()) {
				/*No records in this stream yet, so low and high offset are both 0*/
				return new OffsetAndMetadata(0L);
			}
			@SuppressWarnings("unchecked")
			List<Object> firstRecord = (List<Object>)response.get(0);
			byte[] redisEntryIdBytes = (byte[])firstRecord.get(0);
			String redisEntryId = new String(redisEntryIdBytes);
			long offset = KafkaRedisOffsetMapper.toKafkaOffset(redisEntryId);
			return new OffsetAndMetadata(offset);
		}
	}
	
	private static final byte[] longToBytes(long x) {
	    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
	    buffer.putLong(x);
	    return buffer.array();
	}

	private static final long bytesToLong(byte[] bytes) {
	    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
	    buffer.put(bytes);
	    buffer.flip();//need flip 
	    return buffer.getLong();
	}
	
	private static boolean parseBoolean(String val) {
		if("true".equalsIgnoreCase(val)) {
			return true;
		}
		else if ("false".equalsIgnoreCase(val)) {
			return false;
		}
		else {
			throw new IllegalArgumentException("Invalid value " + val + ", expected either true or false");
		}
	}
	
	public static void main(String args[]) throws Exception {
		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:6379");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Serdes.String().getClass().getName());
        
		ProducerConfig config = new ProducerConfig(props);
		RedisDriver driver = new RedisDriver(config);
		
		TopicPartition tp = new TopicPartition("streams-plaintext-input", 0);
		driver.send(tp, new Header[0], "".getBytes(), "hello world dog cat".getBytes(), System.currentTimeMillis());
		driver.send(tp, new Header[0], "".getBytes(), "dog monkey world".getBytes(), System.currentTimeMillis());
		driver.send(tp, new Header[0], "".getBytes(), "monkey hello".getBytes(), System.currentTimeMillis());
		
		driver.close();
	}

}
