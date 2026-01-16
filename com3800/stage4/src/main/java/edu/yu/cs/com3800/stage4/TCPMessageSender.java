package edu.yu.cs.com3800.stage4;

import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.UDPMessageSender;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TCPMessageSender implements Runnable, LoggingServer {
    private final LinkedBlockingQueue<Message> outgoingMessages;
    private final Logger logger;
    private final int serverTCPPort;
    private final String receiverHost;
    private final int receiverPort;
    private Socket socket;
    public TCPMessageSender(LinkedBlockingQueue<Message> outgoingMessages, int serverTCPPort, String receiverHost, int receiverPort) throws IOException {
        this.outgoingMessages = outgoingMessages;
        this.serverTCPPort = serverTCPPort;
        this.receiverHost = receiverHost;
        this.receiverPort = receiverPort;
        this.logger = initializeLogging(TCPMessageSender.class.getCanonicalName() + "-on-server-with-tcpPort-" + this.serverTCPPort+"-Sending-to-"+this.receiverHost+"-"+this.receiverPort);
        socket = new Socket(receiverHost, receiverPort);
    }

    @Override
    public void run() {
        Thread.currentThread().setName("TCPSender-from-" + serverTCPPort + "-to-" + receiverHost + ":" + receiverPort);
        logger.log(Level.INFO, "TCPMessageSender started, connecting to " +receiverHost + ":" + receiverPort);
        try (DataOutputStream out = new DataOutputStream(socket.getOutputStream())){
            logger.log(Level.INFO, "Connected to " + receiverHost + ":" + receiverPort);
            while(!Thread.currentThread().isInterrupted()){
                try{
                    Message msg = outgoingMessages.take();
                    byte[] payload = msg.getNetworkPayload();
                    out.writeInt(payload.length);//adds the header with the size of the packet so the reciever doesnt read into the next packet
                    out.write(payload);
                    out.flush();
                    this.logger.fine("Message sent:\n" + msg.toString()+"\n");
                }
                catch(IOException e){
                    Thread.currentThread().interrupt();
                    this.logger.log(Level.WARNING,"failed to send packet", e);
                    break;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.log(Level.INFO, "Sender interrupted, exiting");
                    break;
                }
            }
        }
        catch (IOException e) {
            logger.log(Level.SEVERE, "Port " + this.serverTCPPort+ " failed to connect to " +receiverHost + ":" + receiverPort, e);
            throw new RuntimeException("Can't Connect", e);
        }

        logger.log(Level.INFO, "TCPMessageSender exiting");

    }
    public void shutdown() throws IOException {
        Thread.currentThread().interrupt();
        socket.close();
    }

}
