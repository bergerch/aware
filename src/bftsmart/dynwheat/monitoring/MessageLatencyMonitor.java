package bftsmart.dynwheat.monitoring;


import bftsmart.communication.SystemMessage;
import bftsmart.reconfiguration.ServerViewController;

import java.util.*;

public class MessageLatencyMonitor {

    private int window = 1000;
    private ServerViewController controller;
    private ArrayList<TreeMap<Integer, Long>> SentTimes;
    private ArrayList<TreeMap<Integer, Long>> RecvdTimes;
    private int lastQuery = 0;


    MessageLatencyMonitor(ServerViewController controller) {
        this.window = controller.getStaticConf().getMonitoringWindow();
        this.controller = controller;
        int n = controller.getCurrentViewN();
        this.SentTimes = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            SentTimes.add(i, new TreeMap<>());
        }
        this.RecvdTimes = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            RecvdTimes.add(i, new TreeMap<>());
        }
    }

    public synchronized void addSentTime(int replicaID, int monitoringInstanceID, Long timestamp) {
        this.SentTimes.get(replicaID).put(monitoringInstanceID, timestamp);
    }

    public synchronized void addRecvdTime(int replicaID, int monitoringInstanceID, Long timestamp) {
        if (this.SentTimes.get(replicaID).get(monitoringInstanceID) != null) {
            this.RecvdTimes.get(replicaID).put(monitoringInstanceID, timestamp);
        }
    }

    /**
     * @return
     */
    public synchronized Long[] create_M() {
        int n = controller.getCurrentViewN();

        // Initialize latency vector (current replica's perspective of other nodes latencies
        Long[] latency_vector = new Long[n];
        int myself = controller.getStaticConf().getProcessId();

        // Compute latencies to all other nodes
        for (int i = 0; i < n; i++) {

            // within monitoring interval [start = lastQuery; end = lastQuery + window]
            Map<Integer, Long> replicaRecvdTimes = RecvdTimes.get(i).subMap(lastQuery, lastQuery + window);
            Map<Integer, Long> replicaSentTimes = SentTimes.get(i).subMap(lastQuery, lastQuery + window);

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
            Long medianValue = latencies.size() > 0 ? latencies.get(latencies.size()/2) : Long.MAX_VALUE;
            latency_vector[i] = medianValue;
        }
        // Assume self-latency is zero
        latency_vector[myself] = 0L;

        printLatencyVector(latenciesToMillis(latency_vector));

        return latency_vector;
    }

    private double[] latenciesToMillis(Long[] m){
        double[] latencies = new double[m.length];
        for (int i = 0; i < m.length; i++) {
            double latency = Math.round((double) m[i] / 1000.00); // round to precision of micro seconds
            latencies[i] = latency / 1000.00; // convert to milliseconds
        }
        return latencies;
    }

    private void printLatencyVector(double[] m) {
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
