package edu.yu.cs.com3800.stage5;

import java.util.concurrent.atomic.AtomicLong;

public class Heartbeat {
    private final long peerId;
    private volatile AtomicLong sequence = new AtomicLong(0);
    private long currentTime;

    public Heartbeat(long peerId) {
        this.peerId = peerId;
        this.sequence = new AtomicLong(0);
        this.currentTime = System.currentTimeMillis();
    }
    public Heartbeat(long peerId, long sequence) {
        this.peerId = peerId;
        this.sequence = new AtomicLong(sequence);
        this.currentTime = System.currentTimeMillis();
    }
    protected long getPeerId() {
        return this.peerId;
    }
    protected synchronized long getCurrentTime() {
        return currentTime;
    }
    private void setCurrentTime() {
        this.currentTime = System.currentTimeMillis();
    }
    protected void incrementHeartbeat() {
        this.sequence.incrementAndGet();
    }
    protected synchronized void setHeartbeat(long sequence) {
        this.sequence.set(sequence);
        setCurrentTime();
    }
    protected synchronized long getHeartbeat() {
        return this.sequence.get();
    }

    @Override
    public String toString() {
        return "PeerId: " + this.peerId + ", Sequence: " + this.sequence;
    }
}
