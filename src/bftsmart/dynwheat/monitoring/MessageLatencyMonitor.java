package bftsmart.dynwheat.monitoring;

import bftsmart.reconfiguration.ServerViewController;

import java.util.*;

/**
 * This class allows to store and receive a replica's own  monitoring information. Note that all measurement data in here
 * is viewed by the perspective of what a single replica has measured recently by itself without a guarantee to be synchronized yet
 * @author cb
 */
public class MessageLatencyMonitor {

    private int window;
    private ServerViewController controller;
    private ArrayList<TreeMap<Integer, Integer>> sentMsgNonces;

    private ArrayList<TreeMap<Integer, Long>> sentTimestamps;
    private ArrayList<TreeMap<Integer, Long>> recvdTimestamps;
    private int lastQuery = 0;


    /**
     * Creates a new instance of a latency monitor
     *
     * @param controller server view controller
     */
    public MessageLatencyMonitor(ServerViewController controller) {
        this.window = controller.getStaticConf().getMonitoringWindow();
        this.controller = controller;
        int n = controller.getCurrentViewN();
        this.sentTimestamps = new ArrayList<>();
        this.recvdTimestamps = new ArrayList<>();
        this.sentMsgNonces = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            sentTimestamps.add(i, new TreeMap<>());
            recvdTimestamps.add(i, new TreeMap<>());
            sentMsgNonces.add(i, new TreeMap<>());  // Todo only in BFT
        }
    }

    /**
     * Adds a sent timestamp
     *
     * @param replicaID            receiver
     * @param monitoringInstanceID id
     * @param timestamp            time
     */
    public synchronized void addSentTime(int replicaID, int monitoringInstanceID, Long timestamp) {
        this.sentTimestamps.get(replicaID).put(monitoringInstanceID, timestamp);
    }

    /**
     * Adds a sent timestamp with a nonce
     *
     * @param replicaID            receiver
     * @param monitoringInstanceID id
     * @param timestamp            time
     */
    public synchronized void addSentTime(int replicaID, int monitoringInstanceID, Long timestamp, int nonce) {
        this.sentTimestamps.get(replicaID).put(monitoringInstanceID, timestamp);

        // Todo only in BFT:
        this.sentMsgNonces.get(replicaID).put(monitoringInstanceID, nonce);
    }


    /**
     * Adds a received timestamp
     *
     * @param replicaID            sender
     * @param monitoringInstanceID id
     * @param timestamp            time
     */
    public synchronized void addRecvdTime(int replicaID, int monitoringInstanceID, Long timestamp) {
        // Only add a response message timestamp if there is a corresponding sent message
        if (this.sentTimestamps.get(replicaID).get(monitoringInstanceID) != null) { //
            this.recvdTimestamps.get(replicaID).put(monitoringInstanceID, timestamp);
        }
    }

    /**
     * Adds a received timestamp and checks for valid Nonce ... ONLY IN BFT
     *
     * @param replicaID            sender
     * @param monitoringInstanceID id
     * @param timestamp            time
     */
    public synchronized void addRecvdTime(int replicaID, int monitoringInstanceID, Long timestamp, int nonce) {
        // Only add a response message timestamp if there is a corresponding sent message AND nonce was included in response
        if (this.sentTimestamps.get(replicaID).get(monitoringInstanceID) != null
                && this.sentMsgNonces.get(replicaID).get(monitoringInstanceID).equals(nonce)) {
            this.recvdTimestamps.get(replicaID).put(monitoringInstanceID, timestamp);
        }
    }

    /**
     * Creates a latency vector from the current replicas perspective
     *
     * @return latencies to all other nodes
     */
    public synchronized Long[] create_M() {
        int n = controller.getCurrentViewN();

        // Initialize latency vector (current replica's perspective of other nodes latencies
        Long[] latency_vector = new Long[n];
        int myself = controller.getStaticConf().getProcessId();

        // Compute latencies to all other nodes
        for (int i = 0; i < n; i++) {

            // within monitoring interval [start = lastQuery; end = lastQuery + window]
            Map<Integer, Long> replicaRecvdTimes = recvdTimestamps.get(i).subMap(lastQuery, lastQuery + window);
            Map<Integer, Long> replicaSentTimes = sentTimestamps.get(i).subMap(lastQuery, lastQuery + window);

            ArrayList<Long> latencies = new ArrayList<>();
            for (Integer monitoringInstance : replicaSentTimes.keySet()) {
                Long rcvd = replicaRecvdTimes.get(monitoringInstance);
                Long sent = replicaSentTimes.get(monitoringInstance);
                if (rcvd != null) {
                    long latency = (rcvd - sent) / 2; // one-way latency as half of round trip time
                    System.out.println("Latency computed " + (double) Math.round((double) latency / 1000) / 1000.00 + " ms");
                    latencies.add(latency);
                }
            }
            latencies.sort(Comparator.naturalOrder());
            Long medianValue = latencies.size() > 0 ? latencies.get(latencies.size() / 2) : Long.MAX_VALUE;
            latency_vector[i] = medianValue;
        }
        // Assume self-latency is zero
        latency_vector[myself] = 0L;

        // todo lastQuery = lastQuery + window;
        printLatencyVector(latenciesToMillis(latency_vector));

        return latency_vector;
    }

    /**
     * Clears timestamps
     */
    public synchronized void clear() {
        sentTimestamps.clear();
        recvdTimestamps.clear();
        if (sentMsgNonces != null)
            sentMsgNonces.clear();
    }

    public static double[] latenciesToMillis(Long[] m) {
        double[] latencies = new double[m.length];
        for (int i = 0; i < m.length; i++) {
            double latency = Math.round((double) m[i] / 1000.00); // round to precision of micro seconds
            latencies[i] = latency / 1000.00; // convert to milliseconds
        }
        return latencies;
    }

    public static void printLatencyVector(double[] m) {
        System.out.println(".....................Measured latencies .......................");
        System.out.println("    0       1       2        3        4        ....    ");
        System.out.println("...............................................................");
        for (double d : m) {
            System.out.print("  " + d + "  ");
        }
        System.out.println();
        System.out.println("...............................................................");

    }

}
