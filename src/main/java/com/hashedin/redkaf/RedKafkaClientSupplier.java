package com.hashedin.redkaf;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.RedKafAdminClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.consumer.RedKafConsumer;
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor.Assignment;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor.Subscription;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.RedKafProducer;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaClientSupplier;

public class RedKafkaClientSupplier implements KafkaClientSupplier {
	
	@Override
	public AdminClient getAdminClient(Map<String, Object> config) {
		return RedKafAdminClient.create(config);
	}

	@Override
	public Producer<byte[], byte[]> getProducer(Map<String, Object> config) {
		config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, new Serdes.ByteArraySerde().serializer().getClass());
		config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, new Serdes.ByteArraySerde().serializer().getClass());
		
		return new RedKafProducer<byte[], byte[]>(config);
	}

	@Override
	public Consumer<byte[], byte[]> getConsumer(Map<String, Object> config) {
		config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, new Serdes.ByteArraySerde().deserializer().getClass());
		config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, new Serdes.ByteArraySerde().deserializer().getClass());
		return new RedKafConsumer<byte[], byte[]>(config);
	}

	@Override
	public Consumer<byte[], byte[]> getRestoreConsumer(Map<String, Object> config) {
		config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, new Serdes.ByteArraySerde().deserializer().getClass());
		config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, new Serdes.ByteArraySerde().deserializer().getClass());
		return new RedKafConsumer<byte[], byte[]>(config);
	}

	@Override
	public Consumer<byte[], byte[]> getGlobalConsumer(Map<String, Object> config) {
		config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, new Serdes.ByteArraySerde().deserializer().getClass());
		config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, new Serdes.ByteArraySerde().deserializer().getClass());
		return new RedKafConsumer<byte[], byte[]>(config);
	}
}

class MyConsumer implements Consumer<byte[], byte[]> {

	private static final ConsumerRecords<byte[], byte[]> EMPTY = new ConsumerRecords<>(Collections.emptyMap());
	private static final ConsumerRebalanceListener NOOP_LISTENER = new NoOpConsumerRebalanceListener();
	
	private final MockKafka kafka;
	
	private final Set<String> subscriptions;
	private final Set<TopicPartition> assignments;
	private final Map<TopicPartition, MyTopicPartitionState> partitionStates;
	private final List<PartitionAssignor> assignors;
	
	private int state = 0;
	private ConsumerRebalanceListener rebalanceListener;
	
	MyConsumer(MockKafka kafka, List<PartitionAssignor> assignors) {
		this.kafka = kafka;
		this.subscriptions = new HashSet<>();
		this.assignments = new HashSet<>();
		this.assignors = assignors;
		this.partitionStates = new HashMap<>();
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
		Collection<String> topics = kafka.getTopicsMatching(pattern);
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
		return kafka.getClusterMetadata();
	}

	@Override
	public void unsubscribe() {
		subscriptions.clear();
		assignments.clear();
		partitionStates.clear();
	}

	@Override
	public ConsumerRecords<byte[], byte[]> poll(long timeout) {
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
			return kafka.poll(assignments, partitionStates, timeout);
		}
	}

	@Override
	public ConsumerRecords<byte[], byte[]> poll(Duration timeout) {
		return poll(timeout.getSeconds());
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
			state.setOffsetLastCommitted(state.getOffset());
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
			MyTopicPartitionState state = partitionStates.get(tp);
			
			state.setOffsetLastCommitted(meta.offset());
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
			offsets.put(tp, new OffsetAndMetadata(state.getOffsetLastCommitted()));
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
		throw new UnsupportedOperationException("Seek isn't implemented right now");
	}

	@Override
	public void seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) {
		throw new UnsupportedOperationException("Seek isn't implemented right now");		
	}

	@Override
	public void seekToBeginning(Collection<TopicPartition> partitions) {
		throw new UnsupportedOperationException("Seek isn't implemented right now");		
	}

	@Override
	public void seekToEnd(Collection<TopicPartition> partitions) {
		throw new UnsupportedOperationException("Seek isn't implemented right now");		
	}

	@Override
	public long position(TopicPartition partition) {
		return position(partition, null);
	}

	@Override
	public long position(TopicPartition partition, Duration timeout) {
		MyTopicPartitionState partitionState = this.partitionStates.get(partition);
		int position = kafka.position(partition, partitionState);
		return (long)position;
	}

	@Override
	public OffsetAndMetadata committed(TopicPartition partition) {
		return committed(partition, null);
	}

	@Override
	public OffsetAndMetadata committed(TopicPartition partition, Duration timeout) {
		MyTopicPartitionState state = partitionStates.get(partition);
		if (state == null || state.getOffsetLastCommitted() < 0) {
			return null;
		}
		return new OffsetAndMetadata(state.getOffsetLastCommitted());
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
		List<PartitionInfo> partitions = new ArrayList<>();
		for(TopicPartition tp: assignments) {
			if(tp.topic().equals(topic)) {
				partitions.add(kafka.toPartitionInfo(tp));
			}
		}
		return partitions;
	}

	@Override
	public Map<String, List<PartitionInfo>> listTopics() {
		return kafka.listTopics();
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
			Long offset = kafka.getEndOffset(partition);
			offsets.put(partition, offset);
		}
		return offsets;
	}

	@Override
	public void close() {
		System.out.println("Close called");
	}

	@Override
	public void close(long timeout, TimeUnit unit) {
		System.out.println("Close called");		
	}

	@Override
	public void close(Duration timeout) {
		System.out.println("Close called");
		
	}

	@Override
	public void wakeup() {
		System.out.println("Wakeup called");
	}
	
	static class MyTopicPartitionState {
		private long offset = -1L;
		private long offsetLastCommitted = -1L;
		private boolean paused = false;  //whether this partition has been paused by the user
		/* Default offset reset strategy */
	    private final OffsetResetStrategy defaultResetStrategy;
	    
	    public MyTopicPartitionState() {
	    	defaultResetStrategy = OffsetResetStrategy.EARLIEST;
	    }
	    public MyTopicPartitionState(OffsetResetStrategy offsetResetStrategy) {
	    	defaultResetStrategy = offsetResetStrategy;
	    }
		
		public long getOffset() {
			return offset;
		}
		public void setOffset(long offset) {
			this.offset = offset;
		}
		public long getOffsetLastCommitted() {
			return offsetLastCommitted;
		}
		public void setOffsetLastCommitted(long offsetLastCommitted) {
			this.offsetLastCommitted = offsetLastCommitted;
		}
		public boolean isPaused() {
			return paused;
		}
		public void setPaused(boolean paused) {
			this.paused = paused;
		}
		
		public OffsetResetStrategy getOffsetResetStrategy() {
			return defaultResetStrategy;
		}
	}
}

class MyProducer implements Producer<byte[], byte[]> {
	
	private final MockKafka kafka;
	
	public MyProducer(MockKafka kafka) {
		this.kafka = kafka;
	}

	@Override
	public void abortTransaction() throws ProducerFencedException {
		throw new UnsupportedOperationException("Transactions are not supported in Redis");
	}

	@Override
	public void beginTransaction() throws ProducerFencedException {
		throw new UnsupportedOperationException("Transactions are not supported in Redis");		
	}

	@Override
	public void close() {
		//do nothing		
	}

	@Override
	public void close(Duration arg0) {
		//do nothing		
	}

	@Override
	public void commitTransaction() throws ProducerFencedException {
		throw new UnsupportedOperationException("Transactions are not supported in Redis");		
	}

	@Override
	public void flush() {
		//do nothing		
	}

	@Override
	public void initTransactions() {
		throw new UnsupportedOperationException("Transactions are not supported in Redis");		
	}

	@Override
	public Map<MetricName, ? extends Metric> metrics() {
		return Collections.emptyMap();
	}

	@Override
	public List<PartitionInfo> partitionsFor(String topic) {
		return kafka.partitionsFor(topic);
	}

	@Override
	public Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record) {
		return send(record, null);
	}

	@Override
	public Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record, Callback callback) {
		/* TODO: Determine partition based on the record */
		System.out.println(record.topic() + ": " + new String(record.key()) + " = " + Arrays.toString(record.value()));
		
		int partition = 0;
		RecordMetadata meta = kafka.send(record, partition);
		
		if(callback != null) {
			callback.onCompletion(meta, null);
		}
		return new SyncFutureWrapper<RecordMetadata>(meta);
	}

	@Override
	public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> arg0, String arg1)
			throws ProducerFencedException {
		throw new UnsupportedOperationException("Transactions are not supported in Redis");		
	}
}


class SyncFutureWrapper<T> implements Future<T> {
	private final T t;
	public SyncFutureWrapper(T t) {
		this.t = t;
	}

	@Override
	public boolean cancel(boolean arg0) {
		return false;
	}

	@Override
	public T get() throws InterruptedException, ExecutionException {
		return t;
	}

	@Override
	public T get(long arg0, TimeUnit arg1) throws InterruptedException, ExecutionException, TimeoutException {
		return t;
	}

	@Override
	public boolean isCancelled() {
		return false;
	}

	@Override
	public boolean isDone() {
		return true;
	}
	
}