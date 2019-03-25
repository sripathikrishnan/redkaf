package com.hashedin.redkaf;

import java.io.IOException;

import org.apache.kafka.common.serialization.LongDeserializer;

public class Deserializer {

	public static void main(String args[]) throws IOException, ClassNotFoundException {
		byte[] raw = new byte[] {0, 0, 0, 0, 0, 0, 0, 12};
		
		Object value = new LongDeserializer().deserialize("", raw);
		System.out.println(value);
		
	}
}
