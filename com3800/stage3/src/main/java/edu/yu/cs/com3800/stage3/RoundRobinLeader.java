package edu.yu.cs.com3800.stage3;

import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class RoundRobinLeader extends Thread{
    private final ArrayList<InetSocketAddress> servers;
    private final LinkedBlockingQueue<Message> outgoingMessages;
    private final LinkedBlockingQueue<Message> incomingMessages;
    private final String myAddress;
    private final int myPort;
    private long serverToAssignTo = 0;
    private long requestID = 1;
    private final Logger logger;
    private final HashMap<Long, InetSocketAddress> requestIDsTOClientsAddress = new HashMap<>();
    protected RoundRobinLeader(Map<Long, InetSocketAddress> peerIDtoAddress, LinkedBlockingQueue outgoingMessages, LinkedBlockingQueue incomingMessages, InetSocketAddress myAddress, int myPort, Logger  logger) {
        this.outgoingMessages = outgoingMessages;
        this.incomingMessages = incomingMessages;
        this.myAddress = myAddress.getHostString();
        this.myPort = myPort;
        this.logger = logger;
        servers = new ArrayList<>(peerIDtoAddress.values());
        setName("RoundRobinLeader-port-" + this.myPort);

    }

    @Override
    public void run() {
        while(!Thread.currentThread().isInterrupted()){
            if(serverToAssignTo <0){//accounting for overflows, which wont happen but should still be taken into consideration
                serverToAssignTo = 0;
            }

            try {
                Message message = incomingMessages.take();
                if(message.getMessageType()== Message.MessageType.WORK){
                    //keeping the client in the sender slot so the worker knows who the request is from
                    Message outgoingMessage = new Message(message.getMessageType(), message.getMessageContents(),  this.myAddress, this.myPort, this.servers.get((int)serverToAssignTo%servers.size()).getHostString(), this.servers.get((int)serverToAssignTo%servers.size()).getPort(), this.requestID);
                    requestIDsTOClientsAddress.put(requestID, InetSocketAddress.createUnresolved(message.getSenderHost(), message.getSenderPort()));
                    this.outgoingMessages.put(outgoingMessage);
                    this.logger.fine("Received work from "+ requestIDsTOClientsAddress.get(requestID) +". The request ID  is " + requestID+".\nSending work to WORKER " + serverToAssignTo%servers.size() + " who lives at " +this.servers.get((int)serverToAssignTo%servers.size()).getHostString() + ":" +this.servers.get((int)serverToAssignTo%servers.size()).getPort()+"\n");
                    this.requestID++;
                    this.serverToAssignTo++;
                }
                else if (message.getMessageType() == Message.MessageType.COMPLETED_WORK) {
                    if(message.getRequestID()==-1){
                        this.logger.log(Level.INFO, "Worker at " + message.getSenderHost()+":"+message.getSenderPort()+ " sent back an exception");
                    }
                    else{
                        //the message sender host and port is the clients info, sending back to the client
                        InetSocketAddress incomingAddress = requestIDsTOClientsAddress.get(message.getRequestID());
                        if(incomingAddress==null){
                            continue;
                        }
                        String clientHostString = incomingAddress.getHostString();
                        int clientPort = incomingAddress.getPort();
                        this.outgoingMessages.put(new Message(message.getMessageType(), message.getMessageContents(),this.myAddress, this.myPort, clientHostString,clientPort, message.getRequestID()));
                        this.logger.fine("Recieved COMPLETED_WORK with requestID " + message.getRequestID() + " from WORKER  who lives at " + message.getSenderHost()+":"+message.getSenderPort()+"\nSending COMPLETED_WORK back to client at "+ requestIDsTOClientsAddress.get(message.getRequestID())+"\n");
                        this.requestIDsTOClientsAddress.remove(message.getRequestID());
                    }

                }
                else{
                    boolean done = false;
                    while(!done){
                        done = this.incomingMessages.offer(message);
                    }
                    this.logger.warning("Message recieved was not of type WORK or COMPLETED_WORK. Added back to the queue");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                this.logger.log(Level.SEVERE,"Thread interrupted. Stopping RoundRobinLeader.");
            }

        }
    }

    public void shutdown(){
        this.interrupt();
    }
}



