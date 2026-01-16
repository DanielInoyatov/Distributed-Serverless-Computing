package edu.yu.cs.com3800;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**We are implemeting a simplfied version of the election algorithm. For the complete version which covers all possible scenarios, see https://github.com/apache/zookeeper/blob/90f8d835e065ea12dddd8ed9ca20872a4412c78a/zookeeper-server/src/main/java/org/apache/zookeeper/server/quorum/FastLeaderElection.java#L913
 */
public class LeaderElection {
    /**
     * time to wait once we believe we've reached the end of leader election.
     * (when majority is reached
     */
    private final static int finalizeWait = 3200;

    /**
     * Upper bound on the amount of time between two consecutive notification checks.
     * This impacts the amount of time to get the system up again after long partitions. Currently 30 seconds.
     */
    private final static int maxNotificationInterval = 30000;
    private PeerServer server;
    LinkedBlockingQueue<Message> incomingMessagesUDP;
    private final Logger logger;
    private long proposedLeader;
    private long proposedEpoch;
    private HashMap<Long, ElectionNotification> votes = new HashMap<>();
    private LinkedBlockingQueue<Message> otherMessages = new LinkedBlockingQueue<>();

    public LeaderElection(PeerServer server, LinkedBlockingQueue<Message> incomingMessagesUDP, Logger logger) {
        this.server = server;
        this.incomingMessagesUDP = incomingMessagesUDP;
        this.logger = logger;
        if(this.server.getPeerState()== PeerServer.ServerState.OBSERVER){
            this.proposedLeader = -1L;
            this.proposedEpoch = -1L;
        }
        else{
            this.proposedLeader = this.server.getServerId();
            this.proposedEpoch = this.server.getPeerEpoch();
        }
    }

    private void sendNotifications(){
        if(this.server.getPeerState()!=PeerServer.ServerState.LOOKING && this.server.getPeerState()!=PeerServer.ServerState.OBSERVER ){//this makes sure that the server state is actually looking before submitting votes
            this.logger.log(Level.SEVERE,"A vote is being broadcasted yet the server isn't LOOKING for a new leader\n");
            return;
        }
        else{
            this.logger.fine("Server " + this.server.getServerId() + " broadcasting vote for " + this.proposedLeader + "\n");
            ElectionNotification voteBeingSent = new ElectionNotification(this.proposedLeader, this.server.getPeerState(), this.server.getServerId(), this.server.getPeerEpoch());
            this.server.sendBroadcast(Message.MessageType.ELECTION, buildMsgContent(voteBeingSent));
            this.votes.put(server.getServerId(), voteBeingSent);
        }
    }
    /**
     * Note that the logic in the comments below does NOT cover every last "technical" detail you will need to address to implement the election algorithm.
     * How you store all the relevant state, etc., are details you will need to work out.
     * @return the elected leader
     */
    public synchronized Vote lookForLeader() {
        try {
            int notificationInterval = 100;
            //send initial notifications to get things started
            sendNotifications();
            //Loop in which we exchange notifications with other servers until we find a leader
            while(!Thread.currentThread().isInterrupted()){
                //Remove next notification from queue
                Message currentMsg = incomingMessagesUDP.poll(notificationInterval, TimeUnit.MILLISECONDS);

                //If no notifications received...
                //...resend notifications to prompt a reply from others
                //...use exponential back-off when notifications not received but no longer than maxNotificationInterval...
                if(currentMsg==null){
                    this.logger.log(Level.WARNING,"No message received from the server in the last " + notificationInterval/1000.0 + " seconds. Rebroadcasting vote\n" );
                    sendNotifications();
                    notificationInterval = Math.min(notificationInterval*2, maxNotificationInterval);
                }
                else if(this.server.isPeerDead(new InetSocketAddress(currentMsg.getSenderHost(), currentMsg.getSenderPort()))){
                    continue;
                }
                //If we did get a message...
                //...if it's for an earlier epoch, or from an observer, ignore it.
                //...if the received message has a vote for a leader which supersedes mine, change my vote (and send notifications to all other voters about my new vote).
                //(Be sure to keep track of the votes I received and who I received them from.)
                //If I have enough votes to declare my currently proposed leader as the leader...
                //..do a last check to see if there are any new votes for a higher ranked possible leader. If there are, continue in my election "while" loop.
                //If there are no new relevant message from the reception queue, set my own state to either LEADING or FOLLOWING and RETURN the elected leader.
                else if(currentMsg.getMessageType()==Message.MessageType.ELECTION){
                    notificationInterval= 100;
                    ElectionNotification currentNotification = getNotificationFromMessage(currentMsg);
                    this.logger.fine("Received election message from server " + currentNotification.getSenderID() +" for vote "+ currentNotification.getProposedLeaderID() +"\n");
                    if(currentNotification.getState() == PeerServer.ServerState.OBSERVER) {
                        InetSocketAddress observerAddress = this.server.getPeerByID(currentNotification.getSenderID());
                        if(observerAddress != null) {
                            //this.observersSet.add(observerAddress);
                            this.logger.fine("Added observer " + currentNotification.getSenderID() + " to observer set");
                        }
                    }
                    if(!(currentNotification.getPeerEpoch()<this.proposedEpoch)){
                        if(currentNotification.getPeerEpoch()>this.proposedEpoch){
                            this.votes.clear();
                            this.proposedLeader = currentNotification.getProposedLeaderID();
                            this.proposedEpoch = currentNotification.getPeerEpoch();
                            sendNotifications();
                        }
                        else if(supersedesCurrentVote(currentNotification.getProposedLeaderID(), currentNotification.getPeerEpoch())){
                            this.proposedLeader = currentNotification.getProposedLeaderID();
                            sendNotifications();
                        }
                        this.votes.put(currentNotification.getSenderID(), currentNotification);
                    }
                    Vote currentVote = new Vote(this.proposedLeader, this.proposedEpoch);
                    boolean foundBetter = false;
                    if(haveEnoughVotes(votes,currentVote)){
                        long startTime = System.currentTimeMillis();
                        long endTime = System.currentTimeMillis();
                        while(endTime-startTime<finalizeWait) {
                            Message nextMsg = incomingMessagesUDP.poll(finalizeWait - (System.currentTimeMillis() - startTime), TimeUnit.MILLISECONDS);
                            if(nextMsg != null && nextMsg.getMessageType() == Message.MessageType.ELECTION) {
                                ElectionNotification nextNotification = getNotificationFromMessage(nextMsg);
                                if(supersedesCurrentVote(nextNotification.getProposedLeaderID(), nextNotification.getPeerEpoch())){
                                    if(nextNotification.getPeerEpoch()>this.proposedEpoch){
                                        votes.clear();
                                        this.proposedEpoch = nextNotification.getPeerEpoch();
                                    }
                                    this.proposedLeader = nextNotification.getProposedLeaderID();
                                    sendNotifications();
                                    foundBetter =true;
                                    break;
                                }
                            }
                            endTime = System.currentTimeMillis();
                        }
                        if(!foundBetter){
                            acceptElectionWinner(currentNotification);
                            this.logger.info("Server " + this.server.getServerId() + " accepted leader " + this.proposedLeader);
                            if(this.server.getServerId()==this.proposedLeader){
                                this.logger.fine("I am now a MASTER\n");
                            }
                            else if(this.server.getPeerState()!=PeerServer.ServerState.OBSERVER){
                                this.logger.fine("I am now a WORKER\n");
                            }

                            return currentVote;
                        }
                    }

                }
                else{
                    this.otherMessages.add(currentMsg);
                }
            }

        }
        catch (Exception e) {
            this.logger.log(Level.SEVERE,"Exception occurred during election; election canceled",e);
        }
        return null;
    }

    private Vote acceptElectionWinner(ElectionNotification n) {
        //set my state to either LEADING or FOLLOWING
        //clear out the incoming queue before returning

        if(this.server.getPeerState()!=PeerServer.ServerState.LOOKING && this.server.getPeerState()!= PeerServer.ServerState.OBSERVER){
            throw new IllegalStateException("Server state is not LOOKING"+ "\nServer is "+ this.server.getPeerState());
        }
        else{
            incomingMessagesUDP.removeIf(msg -> msg.getMessageType() == Message.MessageType.ELECTION);
            Vote output = new Vote(n.getProposedLeaderID(),n.getPeerEpoch());
            try{
                this.server.setCurrentLeader(output);
            }
            catch (IOException e ){
                throw new RuntimeException("Failed to set leader", e);
            }
            if(this.server.getPeerState()== PeerServer.ServerState.LOOKING){
                if(this.proposedLeader==this.server.getServerId()){
                    this.server.setPeerState(PeerServer.ServerState.LEADING);
                    //notifyObserversOfElectionLeaders();
                }
                else{
                    this.server.setPeerState(PeerServer.ServerState.FOLLOWING);
                }
            }

            Message m;
            try{
                while ((m = otherMessages.poll()) != null) {
                    incomingMessagesUDP.put(m);
                }
            }
            catch (InterruptedException e){
                Thread.currentThread().interrupt();
            }
            return output;
        }
    }

    /*
     * We return true if one of the following two cases hold:
     * 1- New epoch is higher
     * 2- New epoch is the same as current epoch, but server id is higher.
     */
    protected boolean supersedesCurrentVote(long newId, long newEpoch) {
        return (newEpoch > this.proposedEpoch) || ((newEpoch == this.proposedEpoch) && (newId > this.proposedLeader));
    }

    /**
     * Termination predicate. Given a set of votes, determines if we have sufficient support for the proposal to declare the end of the election round.
     * Who voted for who isn't relevant, we only care that each server has one current vote.
     */
    protected boolean haveEnoughVotes(Map<Long, ElectionNotification> votes, Vote proposal) {
        //is the number of votes for the proposal > the size of my peer server’s quorum?
        int quorom = this.server.getQuorumSize();
        int numVotes = 0;
        for(Map.Entry<Long, ElectionNotification> vote: votes.entrySet()){
            if(vote.getValue().getProposedLeaderID()==proposal.getProposedLeaderID() && vote.getValue().getPeerEpoch()==proposal.getPeerEpoch()){
                numVotes++;
                if(numVotes >= quorom){
                    return true;
                }
            }
        }
        return false;
    }

    public static ElectionNotification getNotificationFromMessage(Message msg){
        if(msg.getMessageType()== Message.MessageType.ELECTION){
            ByteBuffer msgBytes = ByteBuffer.wrap(msg.getMessageContents());
            return new ElectionNotification(msgBytes.getLong(), PeerServer.ServerState.getServerState(msgBytes.getChar()), msgBytes.getLong(), msgBytes.getLong());
        }
        else{
            throw new IllegalArgumentException("Invalid message type.\nExpected message type: ELECTION\n Acutal message type: "+msg.getMessageType());
        }
    }

    public static byte[] buildMsgContent(ElectionNotification notification) {
        //return notification.toString().getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + Character.BYTES+Long.BYTES+Long.BYTES);
        buffer.putLong(notification.getProposedLeaderID());
        buffer.putChar(notification.getState().getChar());
        buffer.putLong(notification.getSenderID());
        buffer.putLong(notification.getPeerEpoch());

        return buffer.array();
    }
    private void notifyObserversOfElectionLeaders(){
        String leader = "LEADER:" + this.proposedLeader;
        this.server.sendBroadcast(Message.MessageType.GOSSIP, leader.getBytes());//l'toelet
    }
}