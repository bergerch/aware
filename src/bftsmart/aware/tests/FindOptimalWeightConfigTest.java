package bftsmart.aware.tests;

import bftsmart.aware.decisions.Simulator;
import bftsmart.aware.decisions.WeightConfiguration;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
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
     * @param args the command line arguments
     * @author cb
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
                {0,	67739,	69185,	93000,	40285},
                {67739,	0,	132581,	92021,	35496},
                {69185,	132581,	0,	156703,	99237},
                {93000,	92021,	156703,	0,	70210},
                {40285,	35496,	99237,	70210,	0}

        };

        long[][] write = {
                {0,	67739,	69185,	93000,	40285},
                {67739,	0,	132581,	92021,	35496},
                {69185,	132581,	0,	156703,	99237},
                {93000,	92021,	156703,	0,	70210},
                {40285,	35496,	99237,	70210,	0}
        };
  /*
        long[][] propose = {
                {0, 3, 3, 3, 3},
                {3, 0, 2, 3, 4},
                {3, 2, 0, 1, 3},
                {3, 3, 1, 0, 2},
                {3, 4, 3, 2, 0}

        };

        long[][] write = {
                {0, 3, 3, 3, 3},
                {3, 0, 2, 3, 4},
                {3, 2, 0, 1, 3},
                {3, 3, 1, 0, 2},
                {3, 4, 3, 2, 0}
        };
*/
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

        String lines = "";

        long middle = System.nanoTime();
        boolean isSingleRunAlwaysAmortized = true;
        for (WeightConfiguration w : weightConfigs) {
            for (int primary : w.getR_max()) { // Only replicas in R_max will be considered to become leader ?

                Long prediction = simulator.predictLatency(replicaSet, primary, w, propose, write, n, f, delta);
                Long predictAmortized10 = simulator.predictLatency(replicaSet, primary, w, propose, write, n, f, delta, 1000);

                System.out.println("WeightConfig " + w + "with leader " + primary + " has predicted latency of " + prediction + " (single run)");
                if (!prediction.equals(predictAmortized10)) {
                    isSingleRunAlwaysAmortized = false;
                    System.out.println("WeightConfig " + w + "with leader " + primary + " has predicted latency of " + predictAmortized10 + " (predictAmortized10)");
                    prediction = predictAmortized10;
                }

                lines += prediction + "\n";

                if (prediction > worstLatency) {
                    worstLatency = prediction;
                    worstLeader = primary;
                    worst = w;
                }

                if (prediction < bestLatency) {
                    bestLatency = prediction;
                    bestLeader = primary;
                    best = w;
                }
            }
        }
        long end = System.nanoTime();
        if (!isSingleRunAlwaysAmortized){
        //    System.out.println("Single Run is NOT always the same as amortized");
        }

        Writer output;
        try {
            output = new BufferedWriter(new FileWriter("model-predictions" , false));
            output.append(lines);
            output.close();

        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("!!!!!!!!!!!!!!! Something went wrong " + e.getStackTrace());
        }



        System.out.println("-----------------------------------------------------" +
                "---------------------------------------------------------------");
        System.out.println("The best weight configuration is " + best + " with leader " + bestLeader +
                " it achieves consensus in " + bestLatency);
        System.out.println("The worst weight configuration is " + worst + " with leader " + worstLeader +
                " it achieves consensus in " + worstLatency);

        System.out.println("Computed combinations in " + ((double) (middle - start)) / 1000000.00 + " ms");
        System.out.println("Computed prediction model (amortized 100) " + ((double) (end - middle)) / 1000000.00 + " ms");

    }
}