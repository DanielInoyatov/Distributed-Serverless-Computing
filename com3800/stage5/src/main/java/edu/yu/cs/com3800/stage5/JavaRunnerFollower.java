package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.JavaRunner;
import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class JavaRunnerFollower extends Thread implements LoggingServer {
    private LinkedBlockingQueue<Message> outgoingMessagesTCP;
    private LinkedBlockingQueue<Message> incomingMessagesTCP;
    private final String myAddress;
    private final int myPort;
    private PeerServerImpl peerServerImpl;
    private final Logger logger;
    private TCPMessageSender tcpSender;
    private Thread senderTread;
    private final LinkedBlockingQueue<Message>  completedWorkQueue;
    public JavaRunnerFollower(LinkedBlockingQueue<Message> outgoingMessagesTCP, LinkedBlockingQueue<Message> incomingMessagesTCP, String myAddress, int myPort, PeerServerImpl peerServerImpl, LinkedBlockingQueue<Message> completedWorkQueue) {
        this.outgoingMessagesTCP = outgoingMessagesTCP;
        this.incomingMessagesTCP = incomingMessagesTCP;
        this.myAddress = myAddress;
        this.myPort = myPort;
        this.peerServerImpl = peerServerImpl;
        this.completedWorkQueue = completedWorkQueue;
        try{
            this.logger = initializeLogging(JavaRunnerFollower.class.getCanonicalName() + "-on-port-" + this.myPort);
            this.logger.setUseParentHandlers(false);
        }
        catch(IOException e){
            throw new RuntimeException(e);
        }
        try{
            this.tcpSender = new TCPMessageSender(this.outgoingMessagesTCP, this.myPort, this.peerServerImpl.leaderAddress.getHostString(), this.peerServerImpl.leaderAddress.getPort()+2);
            this.senderTread= new Thread(tcpSender);
            this.senderTread.setDaemon(true);
            this.senderTread.setName("TCPSender-at-JavaRunnerFollower-on-port-" + this.myPort);
            this.senderTread.start();

        }
        catch (IOException ex){
            this.logger.warning("Failed to start JavaRunnerFollwer on "+this.myAddress+":"+this.myPort);
            throw new RuntimeException();
        }
        setName("JavaRunnerFollower-port-" + this.myPort);
        setDaemon(true);
    }

    @Override
    public void run(){
        while (!Thread.currentThread().isInterrupted()){
            long requestID =-1;
            try{
                Message message = this.incomingMessagesTCP.take();
                requestID = message.getRequestID();
                if(message.getMessageType()== Message.MessageType.WORK){
                    String receivedCode = new String(message.getMessageContents(), StandardCharsets.UTF_8);
                    this.logger.info("===WORKER RECEIVED CODE===\n" + receivedCode + "\n===END===");
                    JavaRunner jr = new JavaRunner(Paths.get(System.getProperty("user.dir")));
                    String result = "SUCCESS:" + jr.compileAndRun(new ByteArrayInputStream(message.getMessageContents()));
                    if(this.peerServerImpl.getCurrentLeader()==null){
                        this.completedWorkQueue.put(new Message(Message.MessageType.COMPLETED_WORK, result.getBytes(StandardCharsets.UTF_8), myAddress, myPort,  this.peerServerImpl.leaderAddress.getHostString(), this.peerServerImpl.leaderAddress.getPort(), requestID));
                        //put logging here that leader died and queueing completed work for new leader
                    }
                    else{
                        this.outgoingMessagesTCP.put(new Message(Message.MessageType.COMPLETED_WORK, result.getBytes(StandardCharsets.UTF_8), myAddress, myPort,  this.peerServerImpl.leaderAddress.getHostString(), this.peerServerImpl.leaderAddress.getPort(), requestID));
                        this.logger.fine("Work message received for requestID " + requestID+". Work was processed and sent back to the master.\n");
                    }

                }
            }
            catch (InterruptedException e){
                Thread.currentThread().interrupt();
                this.senderTread.interrupt();
                this.logger.log(Level.SEVERE, "Thread interrupted. Stopping JavaRunnerFollower@"+this.myAddress+":"+this.myPort);
            }
            catch (IOException | ReflectiveOperationException | IllegalArgumentException e) {
                try {
                    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                    try(PrintStream printStream = new PrintStream(byteArrayOutputStream)) { //putting this line of code in () so the stream closes itself right after the block
                        e.printStackTrace(printStream);
                    }
                    String stackTrace = byteArrayOutputStream.toString(StandardCharsets.UTF_8);
                    String exceptionMessage = "EXCEPTION:" + e.getMessage() + "\n" + stackTrace;
                    if(this.peerServerImpl.getCurrentLeader()==null){
                        this.completedWorkQueue.put(new Message(Message.MessageType.COMPLETED_WORK, exceptionMessage.getBytes(StandardCharsets.UTF_8), myAddress, myPort,  this.peerServerImpl.leaderAddress.getHostString(), this.peerServerImpl.leaderAddress.getPort(), requestID));
                        //put logging here that leader died and queueing completed work for new leader
                    }
                    else{
                        this.outgoingMessagesTCP.put(new Message(Message.MessageType.COMPLETED_WORK, exceptionMessage.getBytes(StandardCharsets.UTF_8), myAddress, myPort, this.peerServerImpl.leaderAddress.getHostString(), this.peerServerImpl.leaderAddress.getPort(), requestID));
                        this.logger.log(Level.WARNING, "Exception caught, sent info back to the master\n" ,e);
                    }
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    this.logger.log(Level.SEVERE, "Thread interrupted. Stopping JavaRunnerFollower@"+this.myAddress+":"+this.myPort);
                }
            }
        }
    }
    protected LinkedBlockingQueue<Message> getCompletedWork(){
        return this.completedWorkQueue;
    }
    public void shutdown(){
        this.interrupt();
        this.senderTread.interrupt();
    }
}
