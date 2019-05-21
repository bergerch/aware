package bftsmart.aware.decisions;

import bftsmart.reconfiguration.ServerViewController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeSet;

/**
 * The class contains an algorithm to predict the latency of the BFT-SMaRt consensus algorithm based on
 * sever-to-server measurements and configurable voting weights (WHEAT). In particular, it simulates a protocol run
 * given the measured point-to-point latencies and computes the time each replica received a PROPOSE by the leader,
 * and the time at which each client forms a quorum in the WRITE phase. It then computes the time at which a client
 * quorum of replicas is ready to execute the request.
 *
 * @author cb
 */
public class Simulator {

    private ServerViewController viewControl;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

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
        return this.predictLatency(replicaSet, leader, weightConfig, m_propose, m_write, n, f, delta, 1);
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

        return this.predictLatency(replicaSet, leader, weightConfig, m_propose, m_write, n, f, delta, 1);
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
     * @param rounds       number of consensus rounds used for calculation of amortized costs (calculation depth)
     * @return predicted latency of the SMR protocol
     */
    public Long predictLatency(int[] replicaSet, int leader, WeightConfiguration weightConfig, long[][] m_propose,
                               long[][] m_write, int n, int f, int delta, int rounds) {


        long[] consensusTimes = new long[rounds];
        long[] offsets = new long[n];
        boolean isBFT =  (viewControl == null) || viewControl.getStaticConf().isBFT();


        while (rounds > 0) { // If r > 1, compute the amortized consensus latency for multiple times r under the
            // assumptions that a new consensus starts immediately after the last instance finishes

            // Compute weights and quorum
            double V_min = 1.00;
            double V_max = V_min + (double) delta / (double) f;
            double[] V = new double[n];
            double Q_v = isBFT ? 2 * f * V_max + 1 : f * V_max + 1;

            // Assign binary voting weights to replicas
            for (int i : replicaSet)
                V[i] = weightConfig.getR_max().contains(i) ? V_max : V_min;

            // Simulate time every replica has been proposed to by the selected leader
            long[] t_proposed = new long[n];
            long[] t_write_finished = new long[n];
            long[] t_decided = new long[n];

            @SuppressWarnings("unchecked")
            PriorityQueue<Vote>[] writesRcvd = new PriorityQueue[n];

            @SuppressWarnings("unchecked")
            PriorityQueue<Vote>[] acceptRcvd = new PriorityQueue[n];

            // Compute time proposed time for all replicas. the proposed time is the maximum out of two times:
            //  (1) replica i has received the PROPOSE and (2) replica 1 has finished its last consensus
            //                                                 (respected by offsets that express waiting time)
            for (int i : replicaSet) {
                t_proposed[i] = Math.max(offsets[i], m_propose[leader][i]);
                writesRcvd[i] = new PriorityQueue<>();
                acceptRcvd[i] = new PriorityQueue<>();
               // if (rounds==1000)  System.out.println("Propose received " + i + " " + t_proposed[i]);
            }

            // Compute time at which WRITE of replica j arrives at replica i
            for (int i : replicaSet) {
                for (int j : replicaSet) {
                    writesRcvd[i].add(new Vote(j, V[j], t_proposed[j] + m_write[j][i]));
                }
            }

            // Compute time at which replica i will finish its WRITE quorum
            PriorityQueue<Long> readyToExecute = new PriorityQueue<>();
            for (int i : replicaSet) {
                double votes = 0.00;
                Set<Integer> quorumUsed = new TreeSet<>();
                long t_written = Long.MAX_VALUE;
                while (votes < Q_v) {
                    Vote vote = writesRcvd[i].poll();
                    if (vote != null) {
                        votes += vote.weight;
                        t_written = vote.arrivalTime;
                        quorumUsed.add(vote.castBy);
                    }
                }
                readyToExecute.add(t_written);
                t_write_finished[i] = t_written;
               // if (rounds==1000) System.out.println("Written " + i + " " + t_written);
            }

            // Compute time at which ACCEPT of replica j arrives at replica i
            // CFT: we use proposed instead of write_finished because WRITE is skipped
            for (int i : replicaSet) {
                for (int j : replicaSet) {
                    acceptRcvd[i].add(new Vote(j, V[j], (isBFT ? t_write_finished[j] : t_proposed[i]) + m_write[j][i]));
                }
            }

            // Compute time at which replica i decides a value (finishes consensus)
            for (int i : replicaSet) {
                double votes = 0.00;
                while (votes < Q_v) {
                    Vote vote = acceptRcvd[i].poll();
                    if (vote != null) {
                        votes += vote.weight;
                        t_decided[i] = vote.arrivalTime;
                    }
                }
               // if (rounds==1000) System.out.println("Decided " + i + " " + t_decided[i]);

            }
            consensusTimes[rounds - 1] = t_decided[leader];

            // Compute offsets (time the other replicas need to finish their consensus round relative to the leader)
            for (int i = 0; i < n; i++)
                offsets[i] = t_decided[i] > t_decided[leader] ? t_decided[i] - t_decided[leader] : 0L;

            rounds--;
        }

        // Compute amortized consensus latency
        long sum = 0L;
        for (int i = 0; i < consensusTimes.length; i++) {
            sum += consensusTimes[i];
        }
        return sum / consensusTimes.length;
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
