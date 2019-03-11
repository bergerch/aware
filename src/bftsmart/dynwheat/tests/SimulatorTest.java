package bftsmart.dynwheat.tests;

import bftsmart.dynwheat.decisions.Simulator;
import bftsmart.dynwheat.decisions.WeightConfiguration;

/**
 * Tests if the simulation works
 *
 * @author cb
 */
public class SimulatorTest {

    /**
     * Tests if simulator works. For the given params, the result should be 220 ms
     *
     * @author cb
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception {

        Simulator simulator = new Simulator(null);

        int[] replicaSet = {0, 1, 2, 3, 4};
        int leader = 0;
        WeightConfiguration weightConfig = new WeightConfiguration(2, replicaSet);

        long[][] m = {
                {0, 20, 100, 200, 200},
                {20, 0, 100, 200, 200},
                {100, 100, 0, 100, 100},
                {200, 200, 100, 0, 20},
                {200, 200, 100, 20, 0}
        };

        int n = 5;
        int f = 1;
        int delta = 1;

        Long prediction = simulator.predictLatency(replicaSet, leader, weightConfig, m, m, n, f, delta);

        System.out.println("Predicted Latency is " + prediction + " ms");

    }
}