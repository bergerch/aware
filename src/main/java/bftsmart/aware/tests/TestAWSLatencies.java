package bftsmart.aware.tests;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

import bftsmart.aware.decisions.Simulator;
import bftsmart.aware.decisions.Simulator.SimulationRun;

import static java.lang.Thread.sleep;

public class TestAWSLatencies {
    public static void main(String[] args) {

        /**
         * Simulation parameter
         */
        int SIZE = 21; // number of replicas
        String strategy = "";  // "SA" or "Exhaustive"
        /**
         *
         */

        if (args.length > 0) {
            strategy = args[0];
        }
        // Simply copy-pasted from cloudping.io, todo add parsing from some file later
        String aws_regions = "Africa (Cape Town)\n" +
                "af-south-1\tAsia Pacific (Hong Kong)\n" +
                "ap-east-1\tAsia Pacific (Tokyo)\n" +
                "ap-northeast-1\tAsia Pacific (Seoul)\n" +
                "ap-northeast-2\tAsia Pacific (Osaka)\n" +
                "ap-northeast-3\tAsia Pacific (Mumbai)\n" +
                "ap-south-1\tAsia Pacific (Singapore)\n" +
                "ap-southeast-1\tAsia Pacific (Sydney)\n" +
                "ap-southeast-2\tCanada (Central)\n" +
                "ca-central-1\tEU (Frankfurt)\n" +
                "eu-central-1\tEU (Stockholm)\n" +
                "eu-north-1\tEU (Milan)\n" +
                "eu-south-1\tEU (Ireland)\n" +
                "eu-west-1\tEU (London)\n" +
                "eu-west-2\tEU (Paris)\n" +
                "eu-west-3\tMiddle East (Bahrain)\n" +
                "me-south-1\tSA (São Paulo)\n" +
                "sa-east-1\tUS East (N. Virginia)\n" +
                "us-east-1\tUS East (Ohio)\n" +
                "us-east-2\tUS West (N. California)\n" +
                "us-west-1\tUS West (Oregon)\n" +
                "us-west-2";
        String[] clients = aws_regions.split("\t");
        for (int i = 0; i < clients.length; i++) {
            clients[i] = clients[i].replace("\n", " ");
            System.out.println("./data/new/" + clients[i] + ".csv");
        }

        int clientIndex = 0;
        final int parallel = 8;
        final ReentrantLock[] _parallelism_lock = new ReentrantLock[parallel];
        for (int i = 0; i < parallel; i++) {
            _parallelism_lock[i] = new ReentrantLock();
        }

        for (String client : clients) {
            try {
                long[][] m = readMatrix("./data/cloudPing/cloudping.csv", SIZE, SIZE, ","); // input
                int replicaset[] = makeReplicaSet(SIZE);

                int f = (SIZE - 1) / 3;
                int delta = SIZE - (3 * f + 1);
                int u = 2 * f;

                LinkedList<Thread> simulations = new LinkedList<>();

                final boolean[] init = {false};

                while (f > 0) {
                    String finalStrategy = strategy;
                    int finalF = f;
                    int finalDelta = delta;
                    int finalU = u;
                    int finalClientIndex = clientIndex;
                    Thread t = new Thread() {

                        public void run() {

                            SimulationRun sim;

                            _parallelism_lock[finalClientIndex % parallel].lock();

                            if (finalStrategy.equals("SA")) {
                                sim = Simulator.simulatedAnnealing(SIZE, finalF, finalDelta, finalU, replicaset, m, m, 0);
                            } else {
                                sim = Simulator.exhaustiveSearch(SIZE, finalF, finalDelta, finalU, replicaset, m, m, m[finalClientIndex]);
                            }
                            System.out.println("Client-Region: " + client + " Delta: " + finalDelta + "f: " + finalF);

                            synchronized (this) {
                                try {

                                    BufferedWriter w = new BufferedWriter(
                                            new FileWriter("./data/correctable/" + client + ".csv", true)); // output
                                    if (!init[0]) {
                                        w.write("delta, t, consensus, firstResponse, tVmaxP1, 2tVmaxP1, linearizableT \n" );
                                        init[0] = true;
                                    }
                                    w.write(finalDelta + "," + finalF + "," + sim.getSolutionLatency()/100.0 + "," +
                                            sim.getLatencies()[1]/100.0 + "," + sim.getLatencies()[2]/100.0 + "," +
                                            sim.getLatencies()[3]/100.0 + "," + sim.getLatencies()[4]/100.0 + "\n");
                                    w.close();
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                            _parallelism_lock[finalClientIndex % parallel].unlock();
                        }
                    };

                    // Recalc
                    f--;
                    delta = SIZE - (3 * f + 1);
                    u = 2 * f;

                    simulations.add(t);
                }

                int i= 0;
                for (Thread simulation: simulations) {
                    simulation.setPriority(10 -i);
                    i++;;
                    simulation.start();
                    try {
                        Thread.sleep(1000L);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }

            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } finally {
                clientIndex++;
            }
        }
    }

    private static int[] makeReplicaSet(int n) {
        int ls[] = new int[n];
        for (int i = 0; i < n; i++)
            ls[i] = i;
        return ls;
    }

    private static void printmatrix(long[][] m) {
        for (int i = 0; i < m.length; i++) {
            for (int j = 0; j < m[i].length; j++) {
                System.out.print(m[i][j] + " ");
            }
            System.out.println();
        }
    }

    public static long[][] randomMatrix(int size) {
        Random r = new Random();
        long[][] m = new long[size][size];
        for (int i = 0; i < m.length; i++) {
            for (int j = 0; j < m[i].length; j++) {
                m[i][j] = r.nextInt(200);
            }
        }
        return m;
    }

    /**
     * @param filename
     * @param n        - lines
     * @param m        - columns
     * @return
     * @throws IOException
     */
    public static long[][] readMatrix(String filename, int n, int m, String div) throws IOException {

        BufferedReader reader = new BufferedReader(new FileReader(filename));

        long matrix[][] = new long[n][m];

        String line[];

        for (int i = 0; i < n; i++) {
            line = reader.readLine().split(div);
            for (int j = 0; j < m; j++) {
                matrix[i][j] = Math.round(((Double.parseDouble(line[j]) * 100) / 2.0));// multiplication to keep two decimals
            }
        }

        reader.close();

        return matrix;
    }
}