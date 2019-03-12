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
import org.apache.kafka.clients.admin.AlterConfigsOptions;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.AlterReplicaLogDirsOptions;
import org.apache.kafka.clients.admin.AlterReplicaLogDirsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.CreateAclsOptions;
import org.apache.kafka.clients.admin.CreateAclsResult;
import org.apache.kafka.clients.admin.CreateDelegationTokenOptions;
import org.apache.kafka.clients.admin.CreateDelegationTokenResult;
import org.apache.kafka.clients.admin.CreatePartitionsOptions;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteAclsOptions;
import org.apache.kafka.clients.admin.DeleteAclsResult;
import org.apache.kafka.clients.admin.DeleteConsumerGroupsOptions;
import org.apache.kafka.clients.admin.DeleteConsumerGroupsResult;
import org.apache.kafka.clients.admin.DeleteRecordsOptions;
import org.apache.kafka.clients.admin.DeleteRecordsResult;
import org.apache.kafka.clients.admin.DeleteTopicsOptions;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeAclsOptions;
import org.apache.kafka.clients.admin.DescribeAclsResult;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeConfigsOptions;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsOptions;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.DescribeDelegationTokenOptions;
import org.apache.kafka.clients.admin.DescribeDelegationTokenResult;
import org.apache.kafka.clients.admin.DescribeLogDirsOptions;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsOptions;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ElectPreferredLeadersOptions;
import org.apache.kafka.clients.admin.ElectPreferredLeadersResult;
import org.apache.kafka.clients.admin.ExpireDelegationTokenOptions;
import org.apache.kafka.clients.admin.ExpireDelegationTokenResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.admin.RenewDelegationTokenOptions;
import org.apache.kafka.clients.admin.RenewDelegationTokenResult;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor.Assignment;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor.Subscription;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.streams.KafkaClientSupplier;

public class RedKafkaClientSupplier implements KafkaClientSupplier {

	@Override
	public AdminClient getAdminClient(Map<String, Object> config) {
		return new MyAdminClient();
	}

	@Override
	public Producer<byte[], byte[]> getProducer(Map<String, Object> config) {
		return new MyProducer("localhost", 6379, null, "iot-temperature");
	}

	@Override
	public Consumer<byte[], byte[]> getConsumer(Map<String, Object> originals) {
		ConfigDef definition = new ConfigDef()
				.define(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                Type.LIST,
                Collections.singletonList(RangeAssignor.class),
                new ConfigDef.NonNullValidator(),
                Importance.MEDIUM,
                "");
		
		AbstractConfig config = new AbstractConfig(definition, originals);
		List<PartitionAssignor> assignors = config.getConfiguredInstances(
                ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                PartitionAssignor.class);
		
		return new MyConsumer("localhost", 6379, null, "iot-temperature", assignors);
	}

	@Override
	public Consumer<byte[], byte[]> getRestoreConsumer(Map<String, Object> config) {
		return getConsumer(config);
	}

	@Override
	public Consumer<byte[], byte[]> getGlobalConsumer(Map<String, Object> config) {
		return getConsumer(config);
	}
}

class MyAdminClient extends AdminClient {

	@Override
	public void close(Duration timeout) {
		
	}

	@Override
	public CreateTopicsResult createTopics(Collection<NewTopic> newTopics, CreateTopicsOptions options) {
		throw new UnsupportedOperationException("Create Topics not supported");
	}

	@Override
	public DeleteTopicsResult deleteTopics(Collection<String> topics, DeleteTopicsOptions options) {
		throw new UnsupportedOperationException("Delete Topics not supported");
	}

	@Override
	public ListTopicsResult listTopics(ListTopicsOptions options) {
		throw new UnsupportedOperationException("List Topics not supported");
	}

	@Override
	public DescribeTopicsResult describeTopics(Collection<String> topicNames, DescribeTopicsOptions options) {
		throw new UnsupportedOperationException("Described Topics not supported");
	}

	@Override
	public DescribeClusterResult describeCluster(DescribeClusterOptions options) {
		throw new UnsupportedOperationException("Describe Cluster not supported");
	}

	@Override
	public DescribeAclsResult describeAcls(AclBindingFilter filter, DescribeAclsOptions options) {
		throw new UnsupportedOperationException("Describe ACLs not supported");
	}

	@Override
	public CreateAclsResult createAcls(Collection<AclBinding> acls, CreateAclsOptions options) {
		throw new UnsupportedOperationException("Create ACLs not supported");
	}

	@Override
	public DeleteAclsResult deleteAcls(Collection<AclBindingFilter> filters, DeleteAclsOptions options) {
		throw new UnsupportedOperationException("Delete ACLs not supported");
	}

	@Override
	public DescribeConfigsResult describeConfigs(Collection<ConfigResource> resources, DescribeConfigsOptions options) {
		throw new UnsupportedOperationException("Describe Configs not supported");
	}

	@Override
	public AlterConfigsResult alterConfigs(Map<ConfigResource, Config> configs, AlterConfigsOptions options) {
		throw new UnsupportedOperationException("Alter Configs not supported");
	}

	@Override
	public AlterReplicaLogDirsResult alterReplicaLogDirs(Map<TopicPartitionReplica, String> replicaAssignment,
			AlterReplicaLogDirsOptions options) {
		throw new UnsupportedOperationException("Alter Replica Log Dirs not supported");
	}

	@Override
	public DescribeLogDirsResult describeLogDirs(Collection<Integer> brokers, DescribeLogDirsOptions options) {
		throw new UnsupportedOperationException("Describe Log Dirs not supported");
	}

	@Override
	public DescribeReplicaLogDirsResult describeReplicaLogDirs(Collection<TopicPartitionReplica> replicas,
			DescribeReplicaLogDirsOptions options) {
		throw new UnsupportedOperationException("Describe Replica Log Dirs not supported");
	}

	@Override
	public CreatePartitionsResult createPartitions(Map<String, NewPartitions> newPartitions,
			CreatePartitionsOptions options) {
		throw new UnsupportedOperationException("Create Partitions not supported");
	}

	@Override
	public DeleteRecordsResult deleteRecords(Map<TopicPartition, RecordsToDelete> recordsToDelete,
			DeleteRecordsOptions options) {
		throw new UnsupportedOperationException("Delete Records not supported");
	}

	@Override
	public CreateDelegationTokenResult createDelegationToken(CreateDelegationTokenOptions options) {
		throw new UnsupportedOperationException("Create Delegation Token not supported");
	}

	@Override
	public RenewDelegationTokenResult renewDelegationToken(byte[] hmac, RenewDelegationTokenOptions options) {
		throw new UnsupportedOperationException("Renew Delegation Token not supported");
	}

	@Override
	public ExpireDelegationTokenResult expireDelegationToken(byte[] hmac, ExpireDelegationTokenOptions options) {
		throw new UnsupportedOperationException("Expire Delegation Token not supported");
	}

	@Override
	public DescribeDelegationTokenResult describeDelegationToken(DescribeDelegationTokenOptions options) {
		throw new UnsupportedOperationException("Describe Delegation Token not supported");
	}

	@Override
	public DescribeConsumerGroupsResult describeConsumerGroups(Collection<String> groupIds,
			DescribeConsumerGroupsOptions options) {
		throw new UnsupportedOperationException("Describe Consumer Groups not supported");
	}

	@Override
	public ListConsumerGroupsResult listConsumerGroups(ListConsumerGroupsOptions options) {
		throw new UnsupportedOperationException("List Consumer Groups not supported");
	}

	@Override
	public ListConsumerGroupOffsetsResult listConsumerGroupOffsets(String groupId,
			ListConsumerGroupOffsetsOptions options) {
		throw new UnsupportedOperationException("List Consumer Group Offsets not supported");
	}

	@Override
	public DeleteConsumerGroupsResult deleteConsumerGroups(Collection<String> groupIds,
			DeleteConsumerGroupsOptions options) {
		throw new UnsupportedOperationException("Delete Consumer Groups not supported");
	}

	@Override
	public ElectPreferredLeadersResult electPreferredLeaders(Collection<TopicPartition> partitions,
			ElectPreferredLeadersOptions options) {
		throw new UnsupportedOperationException("electPreferredLeaders not supported");
	}

	@Override
	public Map<MetricName, ? extends Metric> metrics() {
		return Collections.emptyMap();
	}
	
}

class MyConsumer implements Consumer<byte[], byte[]> {

	private static final ConsumerRecords<byte[], byte[]> EMPTY = new ConsumerRecords<>(Collections.emptyMap());
	
	private final List<PartitionAssignor> assignors;
	private int state = 0;
	
	private final String host;
	private final int port;
	private final String password;
	private final Node node;
	private final String topic;
	private long offset = 0;
	private ConsumerRebalanceListener rebalanceListener;
	
	public MyConsumer(String host, int port, String password, String topic, List<PartitionAssignor> assignors) {
		this.host = host;
		this.port = port;
		this.password = password;
		this.topic = topic;
		this.node = new Node(1, host, port);
		this.assignors = assignors;
	}

	@Override
	public Set<TopicPartition> assignment() {
		return Collections.unmodifiableSet(new HashSet<TopicPartition>(Arrays.asList(new TopicPartition(topic, 0))));
	}

	@Override
	public Set<String> subscription() {
		return Collections.unmodifiableSet(new HashSet<String>(Arrays.asList(topic)));
	}

	@Override
	public void subscribe(Collection<String> topics) {
		System.out.println("subscribe called");
	}

	@Override
	public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback) {
		System.out.println("subscribe called");
		this.rebalanceListener = callback;
	}

	@Override
	public void assign(Collection<TopicPartition> partitions) {
		System.out.println("Assign called");
	}

	@Override
	public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {
		this.rebalanceListener = callback;
	}

	private void callPartitionAssignors() {
		PartitionAssignor assignor = assignors.get(0);
		
		String consumerId = Integer.toString(this.hashCode());
		Subscription subscription = assignor.subscription(this.subscription());
		Map<String, Assignment> assignments = assignor.assign(getClusterMetadata(), Collections.singletonMap(consumerId, subscription));
		Assignment assignment = assignments.get(consumerId);
		assignor.onAssignment(assignment);
	}

	private Cluster getClusterMetadata() {
		return new Cluster("my-cluster-id", Collections.singleton(node), 
				partitionsFor(this.topic), Collections.emptySet(), Collections.singleton(this.topic));
	}

	@Override
	public void subscribe(Pattern pattern) {
		System.out.println("Subscribe called");
	}

	@Override
	public void unsubscribe() {
		System.out.println("Unsubscribe called");
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
			TopicPartition tp = new TopicPartition(topic, 0);
			rebalanceListener.onPartitionsAssigned(Collections.singleton(tp));
			state++;
			return EMPTY;
		}
		else {
			TopicPartition tp = new TopicPartition(topic, 0);
			List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
			
			records.add(new ConsumerRecord<byte[], byte[]>(topic, 0, offset++, 
					System.currentTimeMillis(), TimestampType.CREATE_TIME,
					0L,
					("key-" + offset).getBytes().length, ("35").getBytes().length,
					("key-" + offset).getBytes(), ("35").getBytes()));
			
			return new ConsumerRecords<>(Collections.singletonMap(tp, records));
		}
	}

	@Override
	public ConsumerRecords<byte[], byte[]> poll(Duration timeout) {
		return poll(timeout.getSeconds());
	}

	@Override
	public void commitSync() {
		throw new UnsupportedOperationException("Commit isn't implemented right now"); 	
	}

	@Override
	public void commitSync(Duration timeout) {
		throw new UnsupportedOperationException("Commit isn't implemented right now");		
	}

	@Override
	public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
		throw new UnsupportedOperationException("Commit isn't implemented right now");		
	}

	@Override
	public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
		throw new UnsupportedOperationException("Commit isn't implemented right now");		
	}

	@Override
	public void commitAsync() {
		throw new UnsupportedOperationException("Commit isn't implemented right now");		
	}

	@Override
	public void commitAsync(OffsetCommitCallback callback) {
		throw new UnsupportedOperationException("Commit isn't implemented right now");		
	}

	@Override
	public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
		throw new UnsupportedOperationException("Commit isn't implemented right now");		
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
		return 0;
	}

	@Override
	public long position(TopicPartition partition, Duration timeout) {
		return 0;
	}

	@Override
	public OffsetAndMetadata committed(TopicPartition partition) {
		return null;
	}

	@Override
	public OffsetAndMetadata committed(TopicPartition partition, Duration timeout) {
		throw new UnsupportedOperationException("committed is not yet implemented");
	}

	@Override
	public Map<MetricName, ? extends Metric> metrics() {
		return Collections.emptyMap();
	}

	@Override
	public List<PartitionInfo> partitionsFor(String topic) {
		PartitionInfo pinfo = new PartitionInfo(topic, 0, node, new Node[]{}, new Node[] {});
		return Arrays.asList(pinfo);
	}

	@Override
	public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
		PartitionInfo pinfo = new PartitionInfo(topic, 0, node, new Node[]{}, new Node[] {});
		return Arrays.asList(pinfo);
	}

	@Override
	public Map<String, List<PartitionInfo>> listTopics() {
		Map<String, List<PartitionInfo>> topics = new HashMap<>();
		topics.put(topic, this.partitionsFor(topic));
		return Collections.unmodifiableMap(topics);
	}

	@Override
	public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
		return listTopics();
	}

	@Override
	public Set<TopicPartition> paused() {
		return Collections.emptySet();
	}

	@Override
	public void pause(Collection<TopicPartition> partitions) {
		System.out.println("pause called");
	}

	@Override
	public void resume(Collection<TopicPartition> partitions) {
		System.out.println("resume called");
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
		System.out.println("beginningOffsets called");
		return null;
	}

	@Override
	public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout) {
		System.out.println("beginningOffsets called");
		return null;
	}

	@Override
	public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
		System.out.println("endOffsets called");
		return null;
	}

	@Override
	public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout) {
		System.out.println("endOffsets called");
		return null;
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
}

class MyProducer implements Producer<byte[], byte[]> {
	
	private final String host;
	private final int port;
	private final String password;
	private final Node node;
	private final String topic;
	private long offset = 0;
	
	public MyProducer(String host, int port, String password, String topic) {
		this.host = host;
		this.port = port;
		this.password = password;
		this.topic = topic;
		this.node = new Node(1, host, port);
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
	public List<PartitionInfo> partitionsFor(String arg0) {
		PartitionInfo pinfo = new PartitionInfo(topic, 0, node, new Node[]{}, new Node[] {});
		return Arrays.asList(pinfo);
	}

	@Override
	public Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record) {
		System.out.println(new String(record.key()) + ", " + new String(record.value()));
		TopicPartition tp = new TopicPartition(this.topic, 0);
		final RecordMetadata meta = new RecordMetadata(tp, 0, offset++, System.currentTimeMillis(), 0L, record.key().length, record.value().length);
		return new SyncFutureWrapper<RecordMetadata>(meta);
	}

	@Override
	public Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record, Callback callback) {
		System.out.println(new String(record.key()) + ", " + new String(record.value()));
		TopicPartition tp = new TopicPartition(this.topic, 0);
		final RecordMetadata meta = new RecordMetadata(tp, 0, offset++, System.currentTimeMillis(), 0L, record.key().length, record.value().length);
		
		callback.onCompletion(meta, null);
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