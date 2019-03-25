package org.apache.kafka.clients;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class SyncFutureWrapper<T> implements Future<T> {
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
