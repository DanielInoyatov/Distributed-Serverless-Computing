package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.Message;

import java.util.concurrent.LinkedBlockingQueue;

public class QueueMsg {
    private LinkedBlockingQueue<String> queue;
    private Message message;

    public QueueMsg(Message message) {
        queue = new LinkedBlockingQueue<>();
        this.message = message;
    }
    protected Message getMessage() {
        return message;
    }
    protected void put(String msg) throws InterruptedException {
        queue.put(msg);
    }
    protected String take() throws InterruptedException {
        return queue.take();
    }
}
