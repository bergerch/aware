package bftsmart.dynwheat.decisions;

import bftsmart.reconfiguration.ServerViewController;

import java.util.PriorityQueue;

public class Simulator {

    private ServerViewController viewControl;

    public Simulator(ServerViewController controller) {
        this.viewControl = controller;
    }


    public Long PredictLatency() {
        int[] replicaSet = viewControl.getCurrentViewProcesses();

        //TODO
        return Long.MAX_VALUE;

    }


    /**
     * Predics the latency of the SMR system for a given weight configuration and leader selection
     *
     * @param replicaSet   all replicas
     * @param leader       selected leader for protocol simulation
     * @param weightConfig weight configuration to be simulated
     * @param m_propose    sanitized PROPOSE latencies
     * @param m_write      sanitized WRITE/ACCEPT latencies
     * @param n            system size
     * @param f            number of faults
     * @param delta        number of additional spare replicas
     * @return
     */
    public Long PredictLatency(int[] replicaSet, int leader, WeightConfiguration weightConfig, long[][] m_propose,
                               long[][] m_write, int n, int f, int delta) {

        double V_min = 1.00;
        double V_max = V_min + (double) delta / (double) f;

        double[] V = new double[n];

        for (int i = 0; i < n; i++)
            V[i] = weightConfig.getR_max().contains(i) ? V_max : V_min;

        long[] t_proposed = new long[n];

        @SuppressWarnings("unchecked")
        PriorityQueue<Vote>[] writesRcvd = new PriorityQueue[n];

        for (int i = 0; i < n; i++) {
            t_proposed[i] = m_propose[leader][i];
            writesRcvd[i] = new PriorityQueue<>();
        }

        for (int i = 0; i < n; i++) {
            for (int j = 0; j < n; j++) {
                writesRcvd[i].add(new Vote(j, V[j], t_proposed[j] + m_write[j][i]));
            }
        }


        // TODO

        return Long.MAX_VALUE;
    }


    public class Vote {

        int castBy;
        double weight;
        long arrivalTime;

        Vote(int castBy, double weight, long arrivalTime) {
            this.castBy = castBy;
            this.weight = weight;
            this.arrivalTime = arrivalTime;
        }

    }

}
