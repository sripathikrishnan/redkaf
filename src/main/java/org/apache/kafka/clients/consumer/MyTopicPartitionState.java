package org.apache.kafka.clients.consumer;

public class MyTopicPartitionState {
	private long offset = -1L;
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
