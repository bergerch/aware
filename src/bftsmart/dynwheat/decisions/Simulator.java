package bftsmart.dynwheat.decisions;

import bftsmart.reconfiguration.ServerViewController;

import java.util.PriorityQueue;

public class Simulator {

    private ServerViewController viewControl;

    public Simulator(ServerViewController controller) {
        this.viewControl = controller;
    }


    /**
     * Wrapper for PredictLatency using current view information and approximating PROPOSE latencies using WRITE latencies
     *
     * @param leader       selected leader for protocol simulation
     * @param weightConfig weight configuration to be simulated
     * @param m            sanitized WRITE latencies
     * @return predicted latency of the SMR protocol
     */
    public Long predictLatency(int leader, WeightConfiguration weightConfig, long[][] m) {

        // Use the PredictLatency Algorithm; M_PROPOSE = M_WRITE
        return predictLatency(leader, weightConfig, m, m);
    }

    /**
     * Wrapper for PredictLatency using current view information
     *
     * @param leader       selected leader for protocol simulation
     * @param weightConfig weight configuration to be simulated
     * @param m_propose    sanitized PROPOSE latencies
     * @param m_write      sanitized WRITE/ACCEPT latencies
     * @return predicted latency of the SMR protocol
     */
    public Long predictLatency(int leader, WeightConfiguration weightConfig, long[][] m_propose, long[][] m_write) {
        int[] replicaSet = viewControl.getCurrentViewProcesses();
        int n = viewControl.getStaticConf().getN();
        int f = viewControl.getStaticConf().getF();
        int delta = viewControl.getStaticConf().getDelta();

        // Use the PredictLatency Algorithm
        return this.predictLatency(replicaSet, leader, weightConfig, m_propose, m_write, n, f, delta);
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
     * @return predicted latency of the SMR protocol
     */
    public Long predictLatency(int[] replicaSet, int leader, WeightConfiguration weightConfig, long[][] m_propose,
                               long[][] m_write, int n, int f, int delta) {

        // Compute weights and quorum
        double V_min = 1.00;
        double V_max = V_min + (double) delta / (double) f;
        double[] V = new double[n];
        double Q_v = 2 * f * V_max + 1;
        if (viewControl != null) { // Set Q_v to BFT or CFT quorum
            Q_v = viewControl.getStaticConf().isBFT() ? 2 * f * V_max + 1 : f * V_max + 1;
        }

        // Assign binary voting weights to replicas
        for (int i : replicaSet)
            V[i] = weightConfig.getR_max().contains(i) ? V_max : V_min;

        // Simulate time every replica has been proposed to by the selected leader
        long[] t_proposed = new long[n];

        @SuppressWarnings("unchecked")
        PriorityQueue<Vote>[] writesRcvd = new PriorityQueue[n];

        for (int i : replicaSet) {
            t_proposed[i] = m_propose[leader][i];
            writesRcvd[i] = new PriorityQueue<>();
        }

        // Compute time at which WRITE of replica j arrives at replica i
        for (int i : replicaSet) {
            for (int j : replicaSet) {
                writesRcvd[i].add(new Vote(j, V[j], t_proposed[j] + m_write[j][i]));
            }
        }

        // Compute time at which replica i will execute a client request and sent a response to the client
        PriorityQueue<Long> readyToExecute = new PriorityQueue<>();
        for (int i : replicaSet) {
            double votes = 0.00;
            long t_written = Long.MAX_VALUE;
            while (votes < Q_v) {
                Vote vote = writesRcvd[i].poll();
                if (vote != null) {
                    votes += vote.weight;
                    t_written = vote.arrivalTime;
                }
            }
            readyToExecute.add(t_written);
        }

        // Compute time at which a client has enough responses to accept a result
        // Note that we ignore latencies between client and replicas and hence estimate the time by computing the
        // time a client quorum of replicas is ready to execute which requires having formed a weighted quorum of Q_v
        int responsesToClient = 0;
        long t_prediction = Long.MAX_VALUE;
        while (responsesToClient < Math.ceil((double) (n + f + 1) / 2.00)) {
            responsesToClient++;
            t_prediction = readyToExecute.poll();
        }

        // The predicted latency according to our considerations in the paper
        return t_prediction;
    }


    public class Vote implements Comparable {

        int castBy; // replicaID which has cast the vote
        double weight; // weight of the vote i.e. V_min or V_max
        long arrivalTime; // timestamp of simulated arrival of the vote

        Vote(int castBy, double weight, long arrivalTime) {
            this.castBy = castBy;
            this.weight = weight;
            this.arrivalTime = arrivalTime;
        }

        @Override
        public int compareTo(Object o) {
            return Long.compare(this.arrivalTime, ((Vote) o).arrivalTime);
        }
    }

}
