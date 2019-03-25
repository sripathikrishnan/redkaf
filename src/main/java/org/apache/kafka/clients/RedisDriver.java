package org.apache.kafka.clients;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.header.Header;

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
	
	private static final ProtocolCommand XADD = new ProtocolCommand() {
		@Override
		public byte[] getRaw() {
			return "xadd".getBytes();
		}
	};
	
	private static final byte[] ASTERISK = "*".getBytes();
	private static final byte[] KEY = "__redkaf_k".getBytes();
	private static final byte[] VALUE = "__redkaf_v".getBytes();
	private static final byte[] TIMESTAMP = "__redkaf_t".getBytes();
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
			jedis.getClient().sendCommand(XADD, redisKey.getBytes(), ASTERISK, 
					KEY, key, VALUE, value, TIMESTAMP, longToBytes(timestamp));
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
	
	public void createTopics(Collection<NewTopic> newTopics, CreateTopicsOptions options) {
		try(Jedis jedis = jedisPool.getResource()) {
			Pipeline pipeline = jedis.pipelined();
			
			for(NewTopic topic : newTopics) {
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

	private String getKeyForTopic(String topic) {
		return TOPIC_KEY_PREFIX + topic;
	}
	
	private String getKeyForTopicPartition(TopicPartition topicPartition) {
		return TOPIC_PARTITION_KEY_PREFIX + topicPartition.topic() + TOPIC_PARTITION_KEY_SUFFIX + topicPartition.partition();
	}

	@Override
	public void close() throws Exception {
		jedisPool.close();
	}
	
	static final byte[] longToBytes(long x) {
	    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
	    buffer.putLong(x);
	    return buffer.array();
	}

	static final long bytesToLong(byte[] bytes) {
	    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
	    buffer.put(bytes);
	    buffer.flip();//need flip 
	    return buffer.getLong();
	}
	
	private boolean parseBoolean(String val) {
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

}
