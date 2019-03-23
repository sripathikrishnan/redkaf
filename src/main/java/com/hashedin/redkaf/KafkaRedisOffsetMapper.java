package com.hashedin.redkaf;

/**
 * Converts a Redis entry id to a Kafka offset and vice versa
 * 
 * A Redis entry id is in the format <millisecondsTime>-<sequenceNumber>. For example 1518951480106-0
 * A Kafka offset is a monotonically increasing Long
 * 
 * In theory, there is no way to convert one to another without loss of information. 
 * 
 * In practice, we make 3 assumptions -
 *  
 * 1. Existing Kafka clients don't care if the offsets have gaps in them. 
 * 		in other words - it is okay to have 4 sequential records with offsets 1, 3, 605, 1094
 * 2. Records created before 1st Jan 2019 or after 1st Jan 2119 are not supported
 * 
 * 3. Only 2^21 entries (approx 2 million) can be inserted in a given millisecond
 * 
 * If any of these assumptions break, this transformation will not work.
 * 
 * long redisTimestamp;
 * long redisSeqNumber;
 * 
 * let customEpoch = 1546300800000 (1st Jan 2019 00:00:00 UTC)
 * let mask = 0x1FFFFF
 * 
 * From Redis Entry Id to Kafka Offset:
 * -----------------------------------
 * long kafkaOffset = ((redisTimestamp - customEpoch) << 21) | (redisSeqNumber | mask)
 * 
 * From Kafka Offset to Redis Entry Id
 * -----------------------------------
 * long redisTimestamp = (kafkaOffset >> 21) + customEpoch
 * long redisSeqNumber = (kafkaoffset | mask)
 * 
 */
public class KafkaRedisOffsetMapper {
	
	private static final long CUSTOM_EPOCH_START = 1546300800000L; //(1st Jan 2019 00:00:00 UTC)
	private static final long CUSTOM_EPOCH_END = 0x3FFFFFFFFFFL + 1546300800000L;
	private static final long MAX_SEQ_NUMBER = 0x1FFFFFL; //(2^21, or approx. 2 million)
	
	public static final long toKafkaOffset(String redisEntryId) {
		if(redisEntryId == null) {
			throw new NullPointerException("Null redis entry Id");
		}
		String tokens[] = redisEntryId.split("-");
		if (tokens.length != 2) {
			throw new IllegalArgumentException("Invalid redis entry Id - " + redisEntryId + ". "
					+ "Expected to be in format <millisecondsTime>-<sequenceNumber>");
		}
		
		long timestamp = parseLong(tokens[0], "Invalid timestamp portion in redis entry id");
		long sequenceNumber = parseLong(tokens[1], "Invalid sequence portion in redis entry id");
		
		return toKafkaOffset(timestamp, sequenceNumber);
	}

	private static long parseLong(String token, String exception) {
		try {
			return Long.parseLong(token);
		}
		catch(NumberFormatException nfe) {
			throw new IllegalArgumentException(exception, nfe);
		}
	}

	public static final long toKafkaOffset(long redisTimestamp, long redisSeqNumber) {
		if (redisTimestamp < CUSTOM_EPOCH_START || redisTimestamp >= CUSTOM_EPOCH_END) {
			throw new IllegalArgumentException("Can only support records created after 1st Jan 2019 00:00:00.000 and before 1st Jan 2119 00:00:00.000");
		}
		
		if (redisSeqNumber > MAX_SEQ_NUMBER) {
			throw new IllegalArgumentException("Cannot handle sequence numbers greater than 2M");
		}
		
		return ((redisTimestamp - CUSTOM_EPOCH_START) << 21) | redisSeqNumber;
	}
	
	public static final String toRedisEntryId(long kafkaOffset) {
		long redisTimestamp = (kafkaOffset >> 21) + CUSTOM_EPOCH_START;
		long redisSeqNumber = kafkaOffset & MAX_SEQ_NUMBER;
		return redisTimestamp + "-" + redisSeqNumber;
	}
}
