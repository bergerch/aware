package bftsmart.aware.tests;

import bftsmart.aware.decisions.Simulator;
import bftsmart.aware.decisions.Simulator.SimulationRun;

import java.io.*;
import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.locks.ReentrantLock;

public class TestWonderProxyLatencies {
    public static void main(String[] args) {

        /**
         * Simulation parameter
         */
        int SIZE = 51; // number of replicas
        String strategy = "SA";  // "SA" or "Exhaustive"

        boolean optimisticMode = false;

        /**
         *
         */

        String client_regions = "Melbourne, LosAngeles, Tokyo, Amsterdam, Frankfurt, NewYork, CapeTown, SaoPaulo, Cairo, Istanbul, Paris, Montreal";
        String[] clients = client_regions.split(",");

        for (int i = 0; i < clients.length; i++) {
            clients[i] = clients[i].replace("\n", " ");
            System.out.println("./data/new/" + clients[i] + ".csv");
        }

        int clientIndex = 0;
        boolean init = true;

        for (String client : clients) {
            try {
                long[][] m = readMatrix("./data/wonderProxy/wonderProxyReplicasLightSpeed.csv", SIZE, SIZE, ","); // input
                long[][] m_clients = readMatrix("./data/wonderProxy/wonderProxyClientsLightSpeed.csv", 12, SIZE, ","); // input

                int replicaset[] = makeReplicaSet(SIZE);

                int f = optimisticMode ? (SIZE - 1) / 6 : (SIZE - 1) / 3; //threshold
                int delta = SIZE - (3 * f + 1);
                int u = 2 * f;
                SimulationRun sim = Simulator.simulatedAnnealing(SIZE, f, delta, u, replicaset, m, m, 0, m_clients[clientIndex], true);

                System.out.println("Client-Region: " + client + " Delta: " + delta + "f: " + f);
                    try {

                        BufferedWriter w = new BufferedWriter(
                                new FileWriter("./data/correctable/latencies.csv", true)); // output
                        if (init) {
                            w.write("region, delta, t, consensus, firstResponse, tVmaxP1, 2tVmaxP1, linearizableT \n" );
                            init = false;
                        }
                        w.write(client + "," + delta + "," + f + "," + sim.getLatencies()[0]/100.0 + "," +
                                sim.getLatencies()[1]/100.0 + "," + sim.getLatencies()[2]/100.0 + "," +
                                sim.getLatencies()[3]/100.0 + "," + sim.getLatencies()[4]/100.0 + "\n");
                        w.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
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