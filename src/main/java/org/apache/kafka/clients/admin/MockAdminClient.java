package org.apache.kafka.clients.admin;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.internals.KafkaFutureImpl;

import com.hashedin.redkaf.MockKafka;

public class MockAdminClient extends AdminClient {

	private final MockKafka kafka;
	
	public MockAdminClient(MockKafka kafka) {
		this.kafka = kafka;
	}

	@Override
	public void close(Duration timeout) {
		
	}

	@Override
	public CreateTopicsResult createTopics(Collection<NewTopic> newTopics, CreateTopicsOptions options) {
		kafka.createTopics(newTopics, options);
		
		Map<String, KafkaFuture<Void>> futures = new HashMap<String, KafkaFuture<Void>>();
		for(NewTopic topic : newTopics) {
			KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
			future.complete(null);
			futures.put(topic.name(), future);
		}
		
		return new CreateTopicsResult(futures);
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
		Map<String, KafkaFuture<TopicDescription>> futures = new HashMap<>();
		for(String topicName : topicNames) {
			List<TopicPartition> partitions = kafka.getTopicPartitions(topicName);
			KafkaFutureImpl<TopicDescription> future = new KafkaFutureImpl<>();
			if(partitions.size() == 0) {
				future.completeExceptionally(new InvalidTopicException("The given topic name '" +
	                    topicName + "' cannot be represented in a request."));
				continue;
			}
			List<TopicPartitionInfo> partitionInfos = new ArrayList<>();
			for(TopicPartition partition : partitions) {
				TopicPartitionInfo tpi = new TopicPartitionInfo(partition.partition(), kafka.leaderNode(), Collections.emptyList(), Collections.emptyList());
				partitionInfos.add(tpi);
			}
			
			boolean isInternalTopic = kafka.isInternalTopic(topicName);
			TopicDescription td = new TopicDescription(topicName, isInternalTopic, partitionInfos);
			
			future.complete(td);
			futures.put(topicName, future);
		}
		return new DescribeTopicsResult(futures);
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
