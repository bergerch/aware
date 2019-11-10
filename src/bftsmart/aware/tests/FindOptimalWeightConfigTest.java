package bftsmart.aware.tests;

import bftsmart.aware.decisions.AwareConfiguration;
import bftsmart.aware.decisions.Simulator;
import bftsmart.aware.decisions.WeightConfiguration;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

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


        int n = 13;
        int f = 3;
        int delta = 3;


        int u = 2 * f;
        int[] replicaSet = new int[n];
        for (int i = 0; i < n; i++) {
            replicaSet[i] = i;
        }

        System.out.println("Traverse all weight configurations and find the optimum");
        System.out.println("----------------------------------------------------------------");



        /*
        long[][] propose = {
                {0,	67739,	69185,	93000,	40285},
                {67739,	0,	132581,	92021,	35496},
                {69185,	132581,	0,	156703,	99237},
                {93000,	92021,	156703,	0,	70210},
                {40285,	35496,	99237,	70210,	0}

        };*/

        // Sydney, Stockholm, California, Tokio, Sao Paulo
        long[][] propose = generateTestM(n);
                /*{
                {0,	318840,	148980,	113120,	316410},
                {318840,	0,	173160,	262680,	233120},
                {148980,	173160,	0,	115960,	195920},
                {113120,	262280,	115960,	0,	288740},
                {316410,	233120,	195920,	288740,	0}

        };*/


        long[][] write = propose;
        /*
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
        System.out.println("Computed combinations in " + ((double) (middle - start)) / 1000000.00 + " ms");


        long t2 = System.nanoTime();
        simulatedAnnealing(n, f, delta, u, replicaSet, propose, write);
        long t3 = System.nanoTime();
        System.out.println("Simulated Annealing in " + ((double) (t3 - t2)) / 1000000.00 + " ms");


        middle = System.nanoTime();
        boolean isSingleRunAlwaysAmortized = true;
        for (WeightConfiguration w : weightConfigs) {
            for (int primary : w.getR_max()) { // Only replicas in R_max will be considered to become leader ?

                Long prediction = simulator.predictLatency(replicaSet, primary, w, propose, write, n, f, delta);
                Long predictAmortized10 = simulator.predictLatency(replicaSet, primary, w, propose, write, n, f, delta, 10);

                //  System.out.println("WeightConfig " + w + "with leader " + primary + " has predicted latency of " + prediction + " (single run)");
                if (!prediction.equals(predictAmortized10)) {
                    isSingleRunAlwaysAmortized = false;
                    //  System.out.println("WeightConfig " + w + "with leader " + primary + " has predicted latency of " + predictAmortized10 + " (predictAmortized10)");
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
        if (!isSingleRunAlwaysAmortized) {
            //    System.out.println("Single Run is NOT always the same as amortized");
        }

        Writer output;
        try {
            output = new BufferedWriter(new FileWriter("model-predictions", false));
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
        System.out.println("Number of configs " + weightConfigs.size() * u);


    }

    private static void simulatedAnnealing(int n, int f, int delta, int u, int[] replicaSet, long[][] propose, long[][] write) {

        Simulator simulator = new Simulator(null);

        // Initialize
        WeightConfiguration w = new WeightConfiguration(u, replicaSet);
        AwareConfiguration x = new AwareConfiguration(w, 0);
        AwareConfiguration best = x;
        long prediction = simulator.predictLatency(replicaSet, x.getLeader(), x.getWeightConfiguration(), propose,
                write, n, f, delta, 100);
        best.setPredictedLatency(prediction);
        x.setPredictedLatency(prediction);

        // Simulated Annealing parameters
        double temp = 120;
        double coolingRate = 0.0055;
        Random random = new Random();


        int counter = 0;
        int counter2 = 0;
        int betterFound = 0;
        while (temp > 0.2) {

            counter++;
            AwareConfiguration y = new AwareConfiguration(x.getWeightConfiguration().deepCopy(), x.getLeader());

            // Create a random variation of configuration x
            int randomReplicaFrom = random.nextInt(u);
            int randomReplicaTo = random.nextInt(n - u);

            TreeSet<Integer> R_max = (TreeSet<Integer>) y.getWeightConfiguration().getR_max();
            TreeSet<Integer> R_min = (TreeSet<Integer>) y.getWeightConfiguration().getR_min();

            Integer max = (Integer) R_max.toArray()[randomReplicaFrom];
            Integer min = (Integer) R_min.toArray()[randomReplicaTo];

            // Get energy of solutions
            Long predictX = x.getPredictedLatency();
            // Swap min and max replica
            if (max.equals(y.getLeader()))
                y.setLeader(min);

            R_max.remove(max);
            R_max.add(min);

            R_min.remove(min);
            R_min.add(max);


            Long predictY = simulator.predictLatency(replicaSet, y.getLeader(), y.getWeightConfiguration(), propose,
                    write, n, f, delta, 10);


            // If the new solution is better, it is accepted
            if (predictY < predictX) {
                x = y;
                x.setPredictedLatency(predictY);
              //  System.out.println("New solution is better");
                betterFound++;

            } else {
                // If the new solution is worse, calculate an acceptance probability
                double rand = Math.random();
              //  System.out.println("predictX, predictY, temp, " + predictX + " " + predictY + " " + temp + " " + " " + (predictX - predictY) / temp + " " + Math.exp((predictX - predictY) / temp) + " " + rand);
                if (Math.exp(-((predictY - predictX) / (temp))) > rand) {
                //    System.out.println("Do it anyways");
                    counter2++;
                    x = y;
                    x.setPredictedLatency(predictY);
                }
            }

            // Record best solution found
            if (predictY < best.getPredictedLatency()) {
                best = y;
                best.setPredictedLatency(predictY);
            }

            // Cool system down
            temp *= 1 - coolingRate;
        }

        System.out.println("Configurations examined: " + counter);
        System.out.println("Better found: " + betterFound);
        System.out.println("Jumps: " + counter2);
        System.out.println("Final solution latency: " + best.getPredictedLatency());
        System.out.println("Best Configuratuon: L" + best.getLeader() + " " + best.getWeightConfiguration());

    }

    private static long[][] generateTestM(int n) {

        long[][] M = new long[n][n];

        for (int i = 0; i < n; i++) {
            for (int j = 0; j < n; j++) {
                long random = (long) (Math.random() * 100);
                M[i][j] = i == j ? 0 : random;
                M[j][i] = i == j ? 0 : random;
            }
        }
        return M;
    }
}