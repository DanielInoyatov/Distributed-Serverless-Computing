package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TCPMessageReceiver implements LoggingServer, Runnable {
    private final ServerSocket serverSocket;
    private final int myPort;
    private final LinkedBlockingQueue<Message> incomingMessages;
    private final Logger logger;
    private volatile AtomicBoolean shutdown =new AtomicBoolean(false);
    private final ExecutorService connectionHandlers = Executors.newCachedThreadPool(
            new ThreadFactory() {
                public Thread newThread(Runnable r) {
                    Thread t = new Thread(r);
                    t.setDaemon(true);
                    t.setName("TCPReceiver-connection-handler");
                    return t;
                }
            }
    );
    public TCPMessageReceiver(LinkedBlockingQueue<Message> incomingMessages, int myPort) throws IOException {
        this.incomingMessages = incomingMessages;
        this.myPort = myPort;
        this.serverSocket = new ServerSocket(myPort);
        this.logger = initializeLogging(TCPMessageReceiver.class.getCanonicalName() + "-on-port-" + this.myPort);
    }

    @Override
    public void run(){
        Thread.currentThread().setName("TCPReceiver-port-" + this.myPort);
        logger.info("TCPMessageReceiver started on port " + myPort);

        while(!shutdown.get() && !Thread.currentThread().isInterrupted()){
            try {
                Socket socket = serverSocket.accept();
                this.logger.log(Level.INFO, "New connection from " + socket.getInetAddress().getHostName()+":"+socket.getPort()+"\n");
                this.connectionHandlers.submit(() -> {
                    try {
                        receive(socket);
                    } catch (IOException e) {
                        logger.log(Level.SEVERE, "Error in receive thread for " + socket.getInetAddress().getHostName() + ":"+socket.getPort(), e);
                    }
                });
            }
            catch (IOException e) {
                if (!shutdown.get()) {
                    logger.log(Level.SEVERE, "Error accepting connection", e);
                }
                break;
            }
        }
        if (!shutdown.get()) {
            try {
                logger.log(Level.WARNING, "TCPMessageReceiver exited unexpectedly, shutting down");
                shutdown();
            }
            catch (IOException e) {
                logger.log(Level.WARNING, "Error during emergency shutdown", e);
            }
        }
        else{
            logger.log(Level.INFO, "TCPMessageReceiver on port " + myPort + " shutting down normally");
        }
    }

    public void shutdown() throws IOException {
        if(shutdown.get())
            return;
        logger.log(Level.INFO, "Shutting down TCPMessageReceiver on port " + myPort);
        shutdown.set(true);
        connectionHandlers.shutdownNow();
        serverSocket.close();
    }
    private void receive(Socket socket) throws IOException {
        String socketAddress = socket.getInetAddress().getHostName()+":"+socket.getPort();
        logger.log(Level.FINE, "Started receiving messages from " + socketAddress);
        try (Socket s = socket; DataInputStream in = new DataInputStream(socket.getInputStream())) {
            while(!shutdown.get() && !Thread.currentThread().isInterrupted()){
                try{
                    int length = in.readInt();
                    logger.log(Level.FINEST, "Receiving message of " + length + " bytes from " + socketAddress);
                    byte[] payload = new byte[length];
                    in.readFully(payload);
                    Message msg = new Message(payload);
                    this.incomingMessages.put(msg);
                    logger.log(Level.FINE, "Received " + msg.getMessageType() +
                            " message from " + socketAddress + "\n"+
                            " (sender ID: " + msg.getSenderHost() + ":" + msg.getSenderPort() +
                            ", receiver ID: " + msg.getReceiverHost() + ":" + msg.getReceiverPort() + ")");
                }
                catch(IOException e){
                    logger.log(Level.WARNING, "Connection error with " + socketAddress + ", closing connection", e);
                    return;
                }
                catch(InterruptedException e){
                    Thread.currentThread().interrupt();
                    logger.log(Level.INFO, "Receive thread interrupted for " + socketAddress + ", exiting");
                    return;
                }
            }
        }
        catch (IOException e) {
            logger.log(Level.WARNING, "Error closing connection to " + socketAddress, e);
        }

    }

}