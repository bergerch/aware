package bftsmart.dynwheat.tests;

import bftsmart.dynwheat.decisions.Simulator;
import bftsmart.dynwheat.decisions.WeightConfiguration;

import java.util.List;

/**
 * Tests finding the best weight configuration and the best leader
 *
 * @author cb
 */
public class FindOptimalWeightConfigTest {

    /**
     * Tests if simulator works. For the given params, the result should be 220 ms
     *
     * @author cb
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception {

        long start = System.nanoTime();

        Simulator simulator = new Simulator(null);

        int[] replicaSet = {0, 1, 2, 3, 4};

        int n = 5;
        int f = 1;
        int u = 2;
        int delta = 1;

        System.out.println("Traverse all weight configurations and find the optimum");
        System.out.println("----------------------------------------------------------------");

        long[][] propose = {
                {0, 20, 50, 70, 30},
                {20, 0, 45, 65, 40},
                {50, 45, 0, 60, 80},
                {70, 65, 60, 0, 30},
                {30, 40, 80, 30, 0}
        };

        long[][] write = {
                {0, 20, 50, 70, 30},
                {20, 0, 45, 65, 40},
                {50, 45, 0, 60, 80},
                {70, 65, 60, 0, 30},
                {30, 40, 80, 30, 0}
        };

        long bestLatency = Long.MAX_VALUE;
        long worstLatency = Long.MIN_VALUE;

        WeightConfiguration best = new WeightConfiguration(u, replicaSet);
        WeightConfiguration worst = new WeightConfiguration(u, replicaSet);

        int bestLeader = 0;
        int worstLeader = 0;

        /**
         * This is where we start our search for the best weight configuration and protocol leader. We will generate
         * all possible weight configs first, then compute the predicted latency for all of them and for all possible
         * leader variants. Note, that this way, we traverse the entire search space. Since we compute combinations,
         * the search space is factorial in N and needs to be handled with a cut & branch heuristic in larger systems
         */
        List<WeightConfiguration> weightConfigs = WeightConfiguration.allPossibleWeightConfigurations(u, replicaSet);

        for (WeightConfiguration w: weightConfigs) {
            for (int primary : w.getR_max()) { // Only replicas in R_max will be considered to become leader ?
                Long predict = simulator.predictLatency(replicaSet, primary, w, propose, write, n, f, delta);
                System.out.println("WeightConfig " + w + "with leader " + primary + " has predicted latency of " + predict);

                if (predict > worstLatency) {
                    worstLatency = predict;
                    worstLeader = primary;
                    worst = w;
                }

                if (predict < bestLatency) {
                    bestLatency = predict;
                    bestLeader = primary;
                    best = w;
                }
            }
        }

        long end = System.nanoTime();

        System.out.println("-----------------------------------------------------" +
                "---------------------------------------------------------------");
        System.out.println("The best weight configuration is " + best + " with leader " + bestLeader +
                " it achieves consensus in " + bestLatency);
        System.out.println("The worst weight configuration is " + worst + " with leader " + worstLeader +
                " it achieves consensus in " + worstLatency);

        System.out.println("Computed in " + ((double) (end-start))/1000000.00 + " ms");

    }
}