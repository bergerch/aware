package bftsmart.dynwheat.monitoring;


import bftsmart.communication.SystemMessage;
import bftsmart.reconfiguration.ServerViewController;

import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

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
        System.out.println("Add Sent Time " + timestamp);
        this.SentTimes.get(replicaID).put(monitoringInstanceID, timestamp);
        System.out.println("Sent times of " + this.SentTimes.get(replicaID));
    }

    public synchronized void addRecvdTime(int replicaID, int monitoringInstanceID, Long timestamp) {
        this.RecvdTimes.get(replicaID).put(monitoringInstanceID, timestamp);
    }

    /**
     * @return
     */
    public synchronized Long[] create_M() {
        System.out.println("!!!!! SENT-TIMES " + SentTimes);
        int n = controller.getCurrentViewN();
        Long[] latency_vector = new Long[n];
        for (int i = 0; i < n; i++) {
            Map<Integer, Long> replicaRecvdTimes = RecvdTimes.get(i);
            Map<Integer, Long> replicaSentTimes = SentTimes.get(i);

            System.out.println("SENT TIMES " + replicaSentTimes);
            long sum = 0;
            int count = 0;
            for (Integer monitoringInstance : replicaSentTimes.keySet()) {
                Long rcvd = replicaRecvdTimes.get(monitoringInstance);
                Long sent = replicaSentTimes.get(monitoringInstance) ;
                if (rcvd != null) {
                    long latency = rcvd - sent;
                    sum += latency;
                    count++;
                    System.out.println("Computed Latency :" + latency);
                }
            }
            latency_vector[i] = sum / count;
        }

        // TODO remove
        System.out.println("..........................................");
        System.out.println("Measured latencies are " + latency_vector);
        System.out.println("............................................");
        return latency_vector;
    }

}
