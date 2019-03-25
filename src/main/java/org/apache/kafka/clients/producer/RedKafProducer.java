package org.apache.kafka.clients.producer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.RedisDriver;
import org.apache.kafka.clients.SyncFutureWrapper;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.internals.ProducerInterceptors;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

/**
 * An implementation of Kafka Producer that writes to Redis Streams instead of Kafka
 * 
 * This is how the mapping works - 
 * 
 * 1. A Kafka Topic is a dictionary in Redis. 
 * 		- The topic name is the key in redis
 *      - The fields of this dict describe the topic
 *      - numPartitions = number of partitions
 *      - isInternal = true or false, to indicate if the topic is internal
 *      - partition.i = name of a redis key whose type=stream
 * 2. A Kafka TopicPartition is represented as a Redis Stream. The key of this redis stream is stored in the topic dictionary 
 * 3. A Kafka record is converted to a a record within a Redis stream. Each record in redis stream has the following layout
 * 		key = serialized key
 *      value = serialized value
 *      timestamp = timestamp from kafka record
 *      headers = serialized headers
 * 4. Offsets are created by Redis Stream automatically. We map redis created offsets to Kafka offsets using a lossy conversion mechanism.
 * 
 * This Producer implementation does NOT support the following - 
 * 1. Idempotent delivery
 * 2. Transactions
 * 3. Metrics
 * 4. ClusterResourceListeners
 * 
 * This Producer implementation will deliver records to Redis synchronously. 
 * Async APIs work, but they are just thin wrappers over the sync apis
 * 
 * This Producer is thread safe. It is safe to use a single instance of this producer across multiple threads.
 * 
 * @author sri
 *
 */
public class RedKafProducer<K, V> implements Producer<K, V> {
	
	private static final int RECORD_OVERHEAD = 20; //rough estimate, doesn't matter much
	private static final AtomicInteger PRODUCER_CLIENT_ID_SEQUENCE = new AtomicInteger(1);
	
	private final RedisDriver driver;
    private final Logger log;
    @SuppressWarnings("unused")
	private final String clientId;
    private final Partitioner partitioner;
    private final int maxRequestSize;
    private final long totalMemorySize;
    @SuppressWarnings("unused")
	private final CompressionType compressionType;
    private final Time time;
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;
    private final ProducerConfig producerConfig;
    private final ProducerInterceptors<K, V> interceptors;
    @SuppressWarnings("unused")
	private final ApiVersions apiVersions;

    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration. Valid configuration strings
     * are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>. Values can be
     * either strings or Objects of the appropriate type (for example a numeric configuration would accept either the
     * string "42" or the integer 42).
     * <p>
     * Note: after creating a {@code KafkaProducer} you must always {@link #close()} it to avoid resource leaks.
     * @param configs   The producer configs
     *
     */
    public RedKafProducer(final Map<String, Object> configs) {
        this(configs, null, null, null, null, null, Time.SYSTEM);
    }

    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration, a key and a value {@link Serializer}.
     * Valid configuration strings are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>.
     * Values can be either strings or Objects of the appropriate type (for example a numeric configuration would accept
     * either the string "42" or the integer 42).
     * <p>
     * Note: after creating a {@code KafkaProducer} you must always {@link #close()} it to avoid resource leaks.
     * @param configs   The producer configs
     * @param keySerializer  The serializer for key that implements {@link Serializer}. The configure() method won't be
     *                       called in the producer when the serializer is passed in directly.
     * @param valueSerializer  The serializer for value that implements {@link Serializer}. The configure() method won't
     *                         be called in the producer when the serializer is passed in directly.
     */
    public RedKafProducer(Map<String, Object> configs, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this(configs, keySerializer, valueSerializer, null, null, null, Time.SYSTEM);
    }

    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration. Valid configuration strings
     * are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>.
     * <p>
     * Note: after creating a {@code KafkaProducer} you must always {@link #close()} it to avoid resource leaks.
     * @param properties   The producer configs
     */
    public RedKafProducer(Properties properties) {
        this(propsToMap(properties), null, null, null, null, null, Time.SYSTEM);
    }

    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration, a key and a value {@link Serializer}.
     * Valid configuration strings are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>.
     * <p>
     * Note: after creating a {@code KafkaProducer} you must always {@link #close()} it to avoid resource leaks.
     * @param properties   The producer configs
     * @param keySerializer  The serializer for key that implements {@link Serializer}. The configure() method won't be
     *                       called in the producer when the serializer is passed in directly.
     * @param valueSerializer  The serializer for value that implements {@link Serializer}. The configure() method won't
     *                         be called in the producer when the serializer is passed in directly.
     */
    public RedKafProducer(Properties properties, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this(propsToMap(properties), keySerializer, valueSerializer, null, null, null,
                Time.SYSTEM);
    }

    // visible for testing
    @SuppressWarnings("unchecked")
    RedKafProducer(Map<String, Object> configs,
                  Serializer<K> keySerializer,
                  Serializer<V> valueSerializer,
                  Metadata metadata,
                  KafkaClient kafkaClient,
                  ProducerInterceptors<K,V> interceptors,
                  Time time) {
    	
        ProducerConfig config = new ProducerConfig(ProducerConfig.addSerializerToConfig(configs, keySerializer,
                valueSerializer));
        driver = new RedisDriver(config);
        
        try {
            Map<String, Object> userProvidedConfigs = config.originals();
            this.producerConfig = config;
            this.time = time;
            String clientId = config.getString(ProducerConfig.CLIENT_ID_CONFIG);
            if (clientId.length() <= 0)
                clientId = "producer-" + PRODUCER_CLIENT_ID_SEQUENCE.getAndIncrement();
            this.clientId = clientId;

            LogContext logContext = new LogContext(String.format("[Producer clientId=%s] ", clientId));
            log = logContext.logger(RedKafProducer.class);
            log.trace("Starting the Kafka producer");

            this.partitioner = config.getConfiguredInstance(ProducerConfig.PARTITIONER_CLASS_CONFIG, Partitioner.class);
            
            if (keySerializer == null) {
                this.keySerializer = config.getConfiguredInstance(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                                                                                         Serializer.class);
                this.keySerializer.configure(config.originals(), true);
            } else {
                config.ignore(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
                this.keySerializer = keySerializer;
            }
            if (valueSerializer == null) {
                this.valueSerializer = config.getConfiguredInstance(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                                                                                           Serializer.class);
                this.valueSerializer.configure(config.originals(), false);
            } else {
                config.ignore(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
                this.valueSerializer = valueSerializer;
            }

            // load interceptors and make sure they get clientId
            userProvidedConfigs.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
            ProducerConfig configWithClientId = new ProducerConfig(userProvidedConfigs, false);
            @SuppressWarnings("rawtypes")
			List<ProducerInterceptor<K, V>> interceptorList = (List) configWithClientId.getConfiguredInstances(
                    ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, ProducerInterceptor.class);
            if (interceptors != null)
                this.interceptors = interceptors;
            else
                this.interceptors = new ProducerInterceptors<>(interceptorList);
            
            this.maxRequestSize = config.getInt(ProducerConfig.MAX_REQUEST_SIZE_CONFIG);
            this.totalMemorySize = config.getLong(ProducerConfig.BUFFER_MEMORY_CONFIG);
            this.compressionType = CompressionType.forName(config.getString(ProducerConfig.COMPRESSION_TYPE_CONFIG));
            this.apiVersions = new ApiVersions();
            config.logUnused();
            log.debug("Kafka producer started");
        } catch (Throwable t) {
            close();
            // now propagate the exception
            throw new KafkaException("Failed to construct kafka producer", t);
        }
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
		try {
			driver.close();
		}
		catch (Exception e) {}
	}

	@Override
	public void close(Duration arg0) {
		close();		
	}

	@Override
	public void commitTransaction() throws ProducerFencedException {
		throw new UnsupportedOperationException("Transactions are not supported in Redis");		
	}

	@Override
	public void flush() {
		//do nothing, since we don't buffer records	
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
		return driver.partitionsFor(topic);
	}

	@Override
	public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
		return send(record, null);
	}

	@Override
	public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
		// intercept the record, which can be potentially modified; this method does not throw exceptions
        ProducerRecord<K, V> interceptedRecord = this.interceptors.onSend(record);
        return doSend(interceptedRecord, callback);
	}
	
    /**
     * Implementation of asynchronously send a record to a topic.
     */
    private Future<RecordMetadata> doSend(ProducerRecord<K, V> record, Callback callback) {
        TopicPartition tp = null;
        try {
            byte[] serializedKey;
            try {
                serializedKey = keySerializer.serialize(record.topic(), record.headers(), record.key());
            } catch (ClassCastException cce) {
                throw new SerializationException("Can't convert key of class " + record.key().getClass().getName() +
                        " to class " + producerConfig.getClass(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG).getName() +
                        " specified in key.serializer", cce);
            }
            byte[] serializedValue;
            try {
                serializedValue = valueSerializer.serialize(record.topic(), record.headers(), record.value());
            } catch (ClassCastException cce) {
                throw new SerializationException("Can't convert value of class " + record.value().getClass().getName() +
                        " to class " + producerConfig.getClass(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG).getName() +
                        " specified in value.serializer", cce);
            }
            int partition = partition(record, serializedKey, serializedValue, driver.getCluster());
            tp = new TopicPartition(record.topic(), partition);

            setReadOnly(record.headers());
            Header[] headers = record.headers().toArray();

            int serializedSize = serializedKey.length + serializedValue.length + RECORD_OVERHEAD;
            ensureValidRecordSize(serializedSize);
            
            long timestamp = record.timestamp() == null ? time.milliseconds() : record.timestamp();
            log.trace("Sending record {} with callback {} to topic {} partition {}", record, callback, record.topic(), partition);
            
            RecordMetadata recordMeta = driver.send(tp, headers, serializedKey, serializedValue, timestamp);
            
            // producer callback will make sure to call both 'callback' and interceptor callback
            Callback interceptCallback = new InterceptorCallback<>(callback, this.interceptors, tp);
            interceptCallback.onCompletion(recordMeta, null);
            
            return new SyncFutureWrapper<RecordMetadata>(recordMeta);
            
        } catch (ApiException e) {
            log.debug("Exception occurred during message send:", e);
            if (callback != null)
                callback.onCompletion(null, e);
            this.interceptors.onSendError(record, tp, e);
            return new FutureFailure(e);
        } catch (BufferExhaustedException e) {
            this.interceptors.onSendError(record, tp, e);
            throw e;
        } catch (KafkaException e) {
            this.interceptors.onSendError(record, tp, e);
            throw e;
        } catch (Exception e) {
            // we notify interceptor about all exceptions, since onSend is called before anything else in this method
            this.interceptors.onSendError(record, tp, e);
            throw e;
        }
    }

	/**
     * computes partition for given record.
     * if the record has partition returns the value otherwise
     * calls configured partitioner class to compute the partition.
     */
    private int partition(ProducerRecord<K, V> record, byte[] serializedKey, byte[] serializedValue, Cluster cluster) {
        Integer partition = record.partition();
        return partition != null ?
                partition :
                partitioner.partition(
                        record.topic(), record.key(), serializedKey, record.value(), serializedValue, cluster);
    }


	@Override
	public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> arg0, String arg1)
			throws ProducerFencedException {
		throw new UnsupportedOperationException("Transactions are not supported in Redis");		
	}
	
    private static Map<String, Object> propsToMap(Properties properties) {
        Map<String, Object> map = new HashMap<>(properties.size());
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            if (entry.getKey() instanceof String) {
                String k = (String) entry.getKey();
                map.put(k, properties.get(k));
            } else {
                throw new ConfigException(entry.getKey().toString(), entry.getValue(), "Key must be a string.");
            }
        }
        return map;
    }
    
    private void setReadOnly(Headers headers) {
        if (headers instanceof RecordHeaders) {
            ((RecordHeaders) headers).setReadOnly();
        }
    }
    
    /**
     * Validate that the record size isn't too large
     */
    private void ensureValidRecordSize(int size) {
        if (size > this.maxRequestSize)
            throw new RecordTooLargeException("The message is " + size +
                    " bytes when serialized which is larger than the maximum request size you have configured with the " +
                    ProducerConfig.MAX_REQUEST_SIZE_CONFIG +
                    " configuration.");
        if (size > this.totalMemorySize)
            throw new RecordTooLargeException("The message is " + size +
                    " bytes when serialized which is larger than the total memory buffer you have configured with the " +
                    ProducerConfig.BUFFER_MEMORY_CONFIG +
                    " configuration.");
    }

    /**
     * A callback called when producer request is complete. It in turn calls user-supplied callback (if given) and
     * notifies producer interceptors about the request completion.
     */
    private static class InterceptorCallback<K, V> implements Callback {
        private final Callback userCallback;
        private final ProducerInterceptors<K, V> interceptors;
        private final TopicPartition tp;

        private InterceptorCallback(Callback userCallback, ProducerInterceptors<K, V> interceptors, TopicPartition tp) {
            this.userCallback = userCallback;
            this.interceptors = interceptors;
            this.tp = tp;
        }

        public void onCompletion(RecordMetadata metadata, Exception exception) {
            metadata = metadata != null ? metadata : new RecordMetadata(tp, -1, -1, RecordBatch.NO_TIMESTAMP, Long.valueOf(-1L), -1, -1);
            this.interceptors.onAcknowledgement(metadata, exception);
            if (this.userCallback != null)
                this.userCallback.onCompletion(metadata, exception);
        }
    }
    
    private static class FutureFailure implements Future<RecordMetadata> {

        private final ExecutionException exception;

        public FutureFailure(Exception exception) {
            this.exception = new ExecutionException(exception);
        }

        @Override
        public boolean cancel(boolean interrupt) {
            return false;
        }

        @Override
        public RecordMetadata get() throws ExecutionException {
            throw this.exception;
        }

        @Override
        public RecordMetadata get(long timeout, TimeUnit unit) throws ExecutionException {
            throw this.exception;
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
}