package bftsmart.dynwheat.monitoring;

import bftsmart.reconfiguration.ServerViewController;

import java.util.*;

/**
 * This class allows to store and receive a replica's own  monitoring information. Note that all measurement data in here
 * is viewed by the perspective of what a single replica has measured recently by itself without a guarantee to be synchronized yet
 *
 * @author cb
 */
public class MessageLatencyMonitor {

    private int window;
    private ServerViewController controller;
    private ArrayList<TreeMap<Integer, Integer>> sentMsgNonces;

    private ArrayList<TreeMap<Integer, Long>> sentTimestamps;
    private ArrayList<TreeMap<Integer, Long>> recvdTimestamps;

    /**
     * Creates a new instance of a latency monitor
     *
     * @param controller server view controller
     */
    public MessageLatencyMonitor(ServerViewController controller) {
        this.window = controller.getStaticConf().getMonitoringWindow();
        this.controller = controller;
        init();
    }

    private void init() {
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
        // Clear old received timestamp from last monitoring window
        Map<Integer, Long> lastWindowRcvd = recvdTimestamps.get(replicaID);
        lastWindowRcvd.remove(monitoringInstanceID);

        this.sentTimestamps.get(replicaID).put(monitoringInstanceID % window , timestamp);
    }

    /**
     * Adds a sent timestamp with a nonce
     *
     * @param replicaID            receiver
     * @param monitoringInstanceID id
     * @param timestamp            time
     */
    public synchronized void addSentTime(int replicaID, int monitoringInstanceID, Long timestamp, int nonce) {
        this.addSentTime(replicaID, monitoringInstanceID, timestamp);

        // Todo only in BFT:
        this.sentMsgNonces.get(replicaID).put(monitoringInstanceID % window, nonce);
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
        if (this.sentTimestamps.get(replicaID).get(monitoringInstanceID % window) != null) { //
            this.recvdTimestamps.get(replicaID).put(monitoringInstanceID % window, timestamp);
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
        if (this.sentTimestamps.get(replicaID).get(monitoringInstanceID % window) != null
                && this.sentMsgNonces.get(replicaID).get(monitoringInstanceID % window).equals(nonce)) {
            this.recvdTimestamps.get(replicaID).put(monitoringInstanceID % window, timestamp);
        }
    }

    /**
     * Creates a latency vector from the current replicas perspective
     *
     * @return latencies to all other nodes
     */
    public synchronized Long[] create_M() {
        long start = System.nanoTime();
        int n = controller.getCurrentViewN();

        // Initialize latency vector (current replica's perspective of other nodes latencies
        Long[] latency_vector = new Long[n];
        int myself = controller.getStaticConf().getProcessId();

        // Compute latencies to all other nodes
        for (int i = 0; i < n; i++) {

            // within monitoring interval [start = lastQuery; end = lastQuery + window]
            Map<Integer, Long> replicaRecvdTimes = recvdTimestamps.get(i).subMap(0,  window); // Todo should not be necessary anymore?
            Map<Integer, Long> replicaSentTimes = sentTimestamps.get(i).subMap(0, window);

            ArrayList<Long> latencies = new ArrayList<>();
            for (Integer monitoringInstance : replicaRecvdTimes.keySet()) {
                Long rcvd = replicaRecvdTimes.get(monitoringInstance);
                Long sent = replicaSentTimes.get(monitoringInstance);
                if (rcvd != null) {
                    long latency = (rcvd - sent) / 2; // one-way latency as half of round trip time
                    // System.out.println("Latency computed " + (double) Math.round((double) latency / 1000) / 1000.00 + " ms");
                    latencies.add(latency);
                }
            }
            latencies.sort(Comparator.naturalOrder());
            // If there are not latencies (e.g. a replica crashed) report with -1 (Failure value)
            Long medianValue = latencies.size() > 0 ? latencies.get(latencies.size() / 2) : Monitor.MISSING_VALUE;
            latency_vector[i] = medianValue;
            // System.out.println("-- Size of " + replicaRecvdTimes.size());
        }
        // Assume self-latency is zero
        latency_vector[myself] = 0L;
        printLatencyVector(latenciesToMillis(latency_vector));

        long end = System.nanoTime();
        System.out.println("Computed median latencies in " + (double)(end-start)/1000000.00 + " ms");
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

        init();
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
            if (d >= 0 && d < Monitor.MISSING_VALUE) {
                System.out.print("  " + d + "  ");
            } else {
                System.out.print("silent");
            }
        }
        System.out.println();
        System.out.println("...............................................................");

    }

}
