package com.hashedin.redkaf;

import java.util.Map;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.WindowStore;

public class RedisStoreBuilder implements StoreBuilder<WindowStore<Bytes, byte[]>> {

	@Override
	public StoreBuilder<WindowStore<Bytes, byte[]>> withCachingEnabled() {
		return this;
	}

	@Override
	public StoreBuilder<WindowStore<Bytes, byte[]>> withCachingDisabled() {
		return this;
	}

	@Override
	public StoreBuilder<WindowStore<Bytes, byte[]>> withLoggingEnabled(Map<String, String> config) {
		return this;
	}

	@Override
	public StoreBuilder<WindowStore<Bytes, byte[]>> withLoggingDisabled() {
		return this;
	}

	@Override
	public WindowStore<Bytes, byte[]> build() {
		
		return null;
	}

	@Override
	public Map<String, String> logConfig() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean loggingEnabled() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public String name() {
		// TODO Auto-generated method stub
		return null;
	}


}
