package com.hashedin.redkaf;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;

import org.apache.kafka.common.serialization.LongDeserializer;

public class Deserializer {

	public static void main(String args[]) throws IOException, ClassNotFoundException {
		byte[] raw = new byte[] {0, 0, 0, 0, 0, 0, 0, 12};
		
		Object value = new LongDeserializer().deserialize("", raw);
		System.out.println(value);
		
	}
}
