package edu.yu.cs.com3800.stage3;

import edu.yu.cs.com3800.JavaRunner;
import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class JavaRunnerFollower extends Thread{
    private LinkedBlockingQueue<Message> outgoingMessages;
    private LinkedBlockingQueue<Message> incomingMessages;
    private final String myAddress;
    private final int myPort;
    private String leaderHost;
    private int leaderPort;
    private final Logger logger;
    public JavaRunnerFollower(LinkedBlockingQueue<Message> outgoingMessages, LinkedBlockingQueue<Message> incomingMessages, String myAddress, int myPort, String leaderHost, int leaderPort, Logger logger) {
        this.outgoingMessages = outgoingMessages;
        this.incomingMessages = incomingMessages;
        this.myAddress = myAddress;
        this.myPort = myPort;
        this.leaderHost = leaderHost;
        this.leaderPort = leaderPort;
        this.logger = logger;
        setName("JavaRunnerFollower-port-" + this.myPort);

    }

    @Override
    public void run(){
        while (!Thread.currentThread().isInterrupted()){
            long requestID =-1;
            try{
                Message message = this.incomingMessages.take();
                requestID = message.getRequestID();
                if(message.getMessageType()== Message.MessageType.WORK){
                    JavaRunner jr = new JavaRunner(Paths.get(System.getProperty("user.dir")));
                    String result = jr.compileAndRun(new ByteArrayInputStream(message.getMessageContents()));
                    this.outgoingMessages.put(new Message(Message.MessageType.COMPLETED_WORK, result.getBytes(StandardCharsets.UTF_8), myAddress, myPort, this.leaderHost, this.leaderPort, message.getRequestID()));
                    this.logger.fine("Work message received for requestID " + requestID+". Work was processed and sent back to the master.\n");
                }
            }
            catch (InterruptedException e){
                Thread.currentThread().interrupt();
                this.logger.log(Level.SEVERE, "Thread interrupted. Stopping JavaRunnerFollower@"+this.myAddress+":"+this.myPort);
            } catch (IOException | ReflectiveOperationException | IllegalArgumentException e) {
                try {
                    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                    try(PrintStream printStream = new PrintStream(byteArrayOutputStream)) { //putting this line of code in () so the stream closes itself right after the block
                        e.printStackTrace(printStream);
                    }
                    String stackTrace = byteArrayOutputStream.toString(StandardCharsets.UTF_8);
                    String exceptionMessage = e.getMessage() + "\n" + stackTrace;
                    this.outgoingMessages.put(new Message(Message.MessageType.COMPLETED_WORK, exceptionMessage.getBytes(StandardCharsets.UTF_8), myAddress, myPort, this.leaderHost, this.leaderPort, requestID));
                    this.logger.log(Level.WARNING, "Exception caught, sent info back to the master\n" ,e);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    this.logger.log(Level.SEVERE, "Thread interrupted. Stopping JavaRunnerFollower@"+this.myAddress+":"+this.myPort);
                }
            }
        }
    }
}
