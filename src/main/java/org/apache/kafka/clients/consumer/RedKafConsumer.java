package org.apache.kafka.clients.consumer;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import org.apache.kafka.clients.RedisDriver;
import org.apache.kafka.clients.consumer.internals.ConsumerInterceptors;
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor.Assignment;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor.Subscription;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

public class RedKafConsumer<K, V> implements Consumer<K, V> {

	private static final ConsumerRebalanceListener NOOP_LISTENER = new NoOpConsumerRebalanceListener();
    private static final AtomicInteger CONSUMER_CLIENT_ID_SEQUENCE = new AtomicInteger(1);

    private final ConsumerRecords<K, V> EMPTY = new ConsumerRecords<>(Collections.emptyMap());
	
    private final RedisDriver driver;
    private final Logger log;
    private final String clientId;
    private String groupId;

    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;
    private final ConsumerInterceptors<K, V> interceptors;

    private final Time time;
    
    private final Set<String> subscriptions;
	private final Set<TopicPartition> assignments;
	private final Map<TopicPartition, MyTopicPartitionState> partitionStates;
	
    private List<PartitionAssignor> assignors;
	private final OffsetResetStrategy defaultOffsetResetStrategy;
	
	private int state = 0;
	private ConsumerRebalanceListener rebalanceListener;

    /**
     * A consumer is instantiated by providing a set of key-value pairs as configuration. Valid configuration strings
     * are documented <a href="http://kafka.apache.org/documentation.html#consumerconfigs" >here</a>. Values can be
     * either strings or objects of the appropriate type (for example a numeric configuration would accept either the
     * string "42" or the integer 42).
     * <p>
     * Valid configuration strings are documented at {@link ConsumerConfig}.
     * <p>
     * Note: after creating a {@code KafkaConsumer} you must always {@link #close()} it to avoid resource leaks.
     *
     * @param configs The consumer configs
     */
    public RedKafConsumer(Map<String, Object> configs) {
        this(configs, null, null);
    }

    /**
     * A consumer is instantiated by providing a set of key-value pairs as configuration, and a key and a value {@link Deserializer}.
     * <p>
     * Valid configuration strings are documented at {@link ConsumerConfig}.
     * <p>
     * Note: after creating a {@code KafkaConsumer} you must always {@link #close()} it to avoid resource leaks.
     *
     * @param configs The consumer configs
     * @param keyDeserializer The deserializer for key that implements {@link Deserializer}. The configure() method
     *            won't be called in the consumer when the deserializer is passed in directly.
     * @param valueDeserializer The deserializer for value that implements {@link Deserializer}. The configure() method
     *            won't be called in the consumer when the deserializer is passed in directly.
     */
    public RedKafConsumer(Map<String, Object> configs,
                         Deserializer<K> keyDeserializer,
                         Deserializer<V> valueDeserializer) {
        this(new ConsumerConfig(ConsumerConfig.addDeserializerToConfig(configs, keyDeserializer, valueDeserializer)),
            keyDeserializer,
            valueDeserializer);
    }

    /**
     * A consumer is instantiated by providing a {@link java.util.Properties} object as configuration.
     * <p>
     * Valid configuration strings are documented at {@link ConsumerConfig}.
     * <p>
     * Note: after creating a {@code KafkaConsumer} you must always {@link #close()} it to avoid resource leaks.
     *
     * @param properties The consumer configuration properties
     */
    public RedKafConsumer(Properties properties) {
        this(properties, null, null);
    }

    /**
     * A consumer is instantiated by providing a {@link java.util.Properties} object as configuration, and a
     * key and a value {@link Deserializer}.
     * <p>
     * Valid configuration strings are documented at {@link ConsumerConfig}.
     * <p>
     * Note: after creating a {@code KafkaConsumer} you must always {@link #close()} it to avoid resource leaks.
     *
     * @param properties The consumer configuration properties
     * @param keyDeserializer The deserializer for key that implements {@link Deserializer}. The configure() method
     *            won't be called in the consumer when the deserializer is passed in directly.
     * @param valueDeserializer The deserializer for value that implements {@link Deserializer}. The configure() method
     *            won't be called in the consumer when the deserializer is passed in directly.
     */
    public RedKafConsumer(Properties properties,
                         Deserializer<K> keyDeserializer,
                         Deserializer<V> valueDeserializer) {
        this(new ConsumerConfig(ConsumerConfig.addDeserializerToConfig(properties, keyDeserializer, valueDeserializer)),
             keyDeserializer, valueDeserializer);
    }

    @SuppressWarnings("unchecked")
    private RedKafConsumer(ConsumerConfig config, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        try {
        	driver = new RedisDriver(config);
            String clientId = config.getString(ConsumerConfig.CLIENT_ID_CONFIG);
            if (clientId.isEmpty())
                clientId = "consumer-" + CONSUMER_CLIENT_ID_SEQUENCE.getAndIncrement();
            this.clientId = clientId;
            this.groupId = config.getString(ConsumerConfig.GROUP_ID_CONFIG);
            LogContext logContext = new LogContext("[Consumer clientId=" + clientId + ", groupId=" + groupId + "] ");
            this.log = logContext.logger(getClass());
            boolean enableAutoCommit = config.getBoolean(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
            if (groupId == null) { // overwrite in case of default group id where the config is not explicitly provided
                if (!config.originals().containsKey(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG))
                    enableAutoCommit = false;
                else if (enableAutoCommit)
                    throw new InvalidConfigurationException(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG + " cannot be set to true when default group id (null) is used.");
            } else if (groupId.isEmpty())
                log.warn("Support for using the empty group id by consumers is deprecated and will be removed in the next major release.");

            log.debug("Initializing the Kafka consumer");
            this.time = Time.SYSTEM;

            // load interceptors and make sure they get clientId
            Map<String, Object> userProvidedConfigs = config.originals();
            userProvidedConfigs.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
            @SuppressWarnings("rawtypes")
			List<ConsumerInterceptor<K, V>> interceptorList = (List) (new ConsumerConfig(userProvidedConfigs, false)).getConfiguredInstances(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
                    ConsumerInterceptor.class);
            this.interceptors = new ConsumerInterceptors<>(interceptorList);
            if (keyDeserializer == null) {
                this.keyDeserializer = config.getConfiguredInstance(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Deserializer.class);
                this.keyDeserializer.configure(config.originals(), true);
            } else {
                config.ignore(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
                this.keyDeserializer = keyDeserializer;
            }
            if (valueDeserializer == null) {
                this.valueDeserializer = config.getConfiguredInstance(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Deserializer.class);
                this.valueDeserializer.configure(config.originals(), false);
            } else {
                config.ignore(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
                this.valueDeserializer = valueDeserializer;
            }

            defaultOffsetResetStrategy = OffsetResetStrategy.valueOf(config.getString(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG).toUpperCase(Locale.ROOT));
            
            this.subscriptions = new HashSet<>();
    		this.assignments = new HashSet<>();
    		this.partitionStates = new HashMap<>();
            this.assignors = config.getConfiguredInstances(
                    ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                    PartitionAssignor.class);


            config.logUnused();
            log.debug("Kafka consumer initialized");
        } catch (Throwable t) {
            // call close methods if internal objects are already constructed; this is to prevent resource leak. see KAFKA-2121
            close();
            // now propagate the exception
            throw new KafkaException("Failed to construct kafka consumer", t);
        }
    }

	@Override
	public Set<TopicPartition> assignment() {
		return Collections.unmodifiableSet(assignments);
	}

	@Override
	public Set<String> subscription() {
		return Collections.unmodifiableSet(subscriptions);
	}

	@Override
	public void subscribe(Collection<String> topics) {
		subscribe(topics, NOOP_LISTENER);
	}

	@Override
	public void subscribe(Pattern pattern) {
		subscribe(pattern, NOOP_LISTENER);
	}
	
	@Override
	public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {
		Collection<String> topics = driver.getTopicsMatching(pattern);
		subscribe(topics, callback);
	}
	
	@Override
	public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback) {
		this.rebalanceListener = callback;
		subscriptions.clear();
		subscriptions.addAll(topics);
		triggerPartitionReassignment();
	}

	@Override
	public void assign(Collection<TopicPartition> partitions) {
		//Not thread safe - this is fine for now
		assignments.clear();
		assignments.addAll(partitions);
		
		partitionStates.clear();
		for(TopicPartition tp : partitions) {
			partitionStates.put(tp, new MyTopicPartitionState());
		}
	}

	private void triggerPartitionReassignment() {
		state = 0;
	}

	private void callPartitionAssignors() {
		PartitionAssignor assignor = assignors.get(0);
		
		String consumerId = Integer.toString(this.hashCode());
		
		/*
		 * For now, we only collect subscriptions from one consumer.
		 * This is the place where we will have to fetch subscriptions from all different Consumers.
		 */
		Subscription subscription = assignor.subscription(this.subscription());
		Map<String, Subscription> allSubscriptions = Collections.singletonMap(consumerId, subscription);
		
		/*
		 * This is where will have to acquire a lock from redis, and then one Consumer will proceed to do the assignments
		 */
		Map<String, Assignment> newAssignments = assignor.assign(getClusterMetadata(), allSubscriptions);
		
		/*
		 * TODO: We will then have to broadcast the new assignments to all the Consumers
		 * 
		 */
		
		/*
		 * Finally, once we receive the new assignments, we will update this Consumer
		 */
		Assignment myAssignment = newAssignments.get(consumerId);
		this.assign(myAssignment.partitions());
		
		assignor.onAssignment(myAssignment);
	}

	private Cluster getClusterMetadata() {
		return driver.getCluster();
	}

	@Override
	public void unsubscribe() {
		subscriptions.clear();
		assignments.clear();
		partitionStates.clear();
	}

	@Override
	public ConsumerRecords<K, V> poll(long timeout) {
		if (state == 0) {
			callPartitionAssignors();
			state++;
			return EMPTY;
		}
		else if (state == 1) {
			rebalanceListener.onPartitionsRevoked(Collections.emptyList());
			state++;
			return EMPTY;
		}
		else if (state == 2) {
			rebalanceListener.onPartitionsAssigned(this.assignments);
			state++;
			return EMPTY;
		}
		else {
			return driver.poll(assignments, partitionStates, timeout,
					keyDeserializer, valueDeserializer);
		}
	}

	@Override
	public ConsumerRecords<K, V> poll(Duration timeout) {
		return poll(timeout.getSeconds() * 1000);
	}

	@Override
	public void commitSync() {
		Duration duration = null;
		commitSync(duration);
	}

	@Override
	public void commitSync(Duration timeout) {
		for(TopicPartition tp : assignments) {
			MyTopicPartitionState state = partitionStates.get(tp);
			driver.commit(groupId, clientId, tp, state.getOffset());
		}		
	}

	@Override
	public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
		commitSync(offsets, null);		
	}

	@Override
	public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
		for(Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
			TopicPartition tp = entry.getKey();
			OffsetAndMetadata meta = entry.getValue();
			driver.commit(groupId, clientId, tp, meta.offset());
		}
	}

	@Override
	public void commitAsync() {
		commitSync();		
	}

	@Override
	public void commitAsync(OffsetCommitCallback callback) {
		Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
		for(TopicPartition tp : assignments) {
			MyTopicPartitionState state = partitionStates.get(tp);
			offsets.put(tp, new OffsetAndMetadata(state.getOffset()));
		}
		commitAsync(offsets, callback);
	}

	@Override
	public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
		commitSync(offsets);
		callback.onComplete(offsets, null);
	}

	@Override
	public void seek(TopicPartition partition, long offset) {
		MyTopicPartitionState state = partitionStates.get(partition);
		state.setOffset(offset);
	}

	@Override
	public void seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) {
		seek(partition, offsetAndMetadata.offset());		
	}

	@Override
	public void seekToBeginning(Collection<TopicPartition> partitions) {
		for(TopicPartition partition : partitions) {
			seek(partition, driver.getLogStartOffset(partition));
		}
	}

	@Override
	public void seekToEnd(Collection<TopicPartition> partitions) {
		for(TopicPartition partition : partitions) {
			seek(partition, driver.getHighWatermark(partition));
		}		
	}

	@Override
	public long position(TopicPartition partition) {
		return position(partition, null);
	}

	@Override
	public long position(TopicPartition partition, Duration timeout) {
		MyTopicPartitionState partitionState = this.partitionStates.get(partition);
		Long offset = partitionState.getOffset();
		if (offset < 0) {
			if (OffsetResetStrategy.EARLIEST.equals(partitionState.getOffsetResetStrategy())) {
				offset = driver.getLogStartOffset(partition).offset();
			}
			else if (OffsetResetStrategy.LATEST.equals(partitionState.getOffsetResetStrategy())) {
				offset = driver.getHighWatermark(partition).offset();
			}
			else {
				throw new NoOffsetForPartitionException(Collections.singleton(partition));
			}
		}
		return offset;
	}

	@Override
	public OffsetAndMetadata committed(TopicPartition partition) {
		return committed(partition, null);
	}

	@Override
	public OffsetAndMetadata committed(TopicPartition partition, Duration timeout) {
		return driver.getCommitted(groupId, clientId, partition);
	}

	@Override
	public Map<MetricName, ? extends Metric> metrics() {
		return Collections.emptyMap();
	}

	@Override
	public List<PartitionInfo> partitionsFor(String topic) {
		return partitionsFor(topic, null);
	}

	@Override
	public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
		return driver.partitionsFor(topic);
	}

	@Override
	public Map<String, List<PartitionInfo>> listTopics() {
		return driver.listTopicsWithPartitions();
	}

	@Override
	public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
		return listTopics();
	}

	@Override
	public Set<TopicPartition> paused() {
		Set<TopicPartition> pausedPartitions = new HashSet<>();
		for(Map.Entry<TopicPartition, MyTopicPartitionState> entry: partitionStates.entrySet()) {
			if(entry.getValue().isPaused()) {
				pausedPartitions.add(entry.getKey());
			}
		}
		return pausedPartitions;
	}

	@Override
	public void pause(Collection<TopicPartition> partitions) {
		pauseOrResume(partitions, true);
	}

	@Override
	public void resume(Collection<TopicPartition> partitions) {
		pauseOrResume(partitions, false);
	}
	
	private void pauseOrResume(Collection<TopicPartition> partitions, boolean newState) {
		for(TopicPartition tp: partitions) {
			if(partitionStates.containsKey(tp)) {
				MyTopicPartitionState state = partitionStates.get(tp);
				state.setPaused(newState);
			}
		}
	}

	@Override
	public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
		System.out.println("offset for time called");
		return null;
	}

	@Override
	public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch,
			Duration timeout) {
		System.out.println("offset for time called");
		return null;
	}

	@Override
	public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
		return beginningOffsets(partitions, null);
	}

	@Override
	public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout) {
		Map<TopicPartition, Long> offsets = new HashMap<>();
		for(TopicPartition partition: partitions) {
			offsets.put(partition, 0L);
		}
		return offsets;
	}

	@Override
	public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
		return endOffsets(partitions, null);
	}

	@Override
	public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout) {
		Map<TopicPartition, Long> offsets = new HashMap<>();
		for(TopicPartition partition: partitions) {
			Long offset = driver.getHighWatermark(partition).offset();
			offsets.put(partition, offset);
		}
		return offsets;
	}

	@Override
	public void close() {
		try {
			driver.close();
		}
		catch (Exception e) {}
	}

	@Override
	public void close(long timeout, TimeUnit unit) {
		close();
	}

	@Override
	public void close(Duration timeout) {
		close();
		
	}

	@Override
	public void wakeup() {
		//do nothing
	}

}