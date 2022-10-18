/**
Copyright (c) 2007-2013 Alysson Bessani, Eduardo Alchieri, Paulo Sousa, and the authors indicated in the @author tags

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package bftsmart.demo.microbenchmarks;

import java.io.IOException;

import bftsmart.tom.AsynchServiceProxy;
import bftsmart.tom.ServiceProxy;
import bftsmart.tom.util.Storage;
import bftsmart.tom.util.TOMUtil;
import ch.qos.logback.core.net.SyslogOutputStream;

import java.io.FileWriter;
import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.Security;
import java.security.Signature;
import java.security.SignatureException;
import java.security.spec.EncodedKeySpec;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import bftsmart.correctable.CorrectableSimple;

/**
 * Example client that updates a BFT replicated service (a counter).
 *
 */
public class ThroughputLatencyClientICGDynamic {

    public static int initId = 0;
    static LinkedBlockingQueue<String> latencies;
    static Thread writerThread;

    /*
     * public static String privKey =
     * "MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgXa3mln4anewXtqrM" +
     * "hMw6mfZhslkRa/j9P790ToKjlsihRANCAARnxLhXvU4EmnIwhVl3Bh0VcByQi2um" +
     * "9KsJ/QdCDjRZb1dKg447voj5SZ8SSZOUglc/v8DJFFJFTfygjwi+27gz";
     * 
     * public static String pubKey =
     * "MIICNjCCAd2gAwIBAgIRAMnf9/dmV9RvCCVw9pZQUfUwCgYIKoZIzj0EAwIwgYEx" +
     * "CzAJBgNVBAYTAlVTMRMwEQYDVQQIEwpDYWxpZm9ybmlhMRYwFAYDVQQHEw1TYW4g" +
     * "RnJhbmNpc2NvMRkwFwYDVQQKExBvcmcxLmV4YW1wbGUuY29tMQwwCgYDVQQLEwND" +
     * "T1AxHDAaBgNVBAMTE2NhLm9yZzEuZXhhbXBsZS5jb20wHhcNMTcxMTEyMTM0MTEx" +
     * "WhcNMjcxMTEwMTM0MTExWjBpMQswCQYDVQQGEwJVUzETMBEGA1UECBMKQ2FsaWZv" +
     * "cm5pYTEWMBQGA1UEBxMNU2FuIEZyYW5jaXNjbzEMMAoGA1UECxMDQ09QMR8wHQYD" +
     * "VQQDExZwZWVyMC5vcmcxLmV4YW1wbGUuY29tMFkwEwYHKoZIzj0CAQYIKoZIzj0D" +
     * "AQcDQgAEZ8S4V71OBJpyMIVZdwYdFXAckItrpvSrCf0HQg40WW9XSoOOO76I+Umf" +
     * "EkmTlIJXP7/AyRRSRU38oI8Ivtu4M6NNMEswDgYDVR0PAQH/BAQDAgeAMAwGA1Ud" +
     * "EwEB/wQCMAAwKwYDVR0jBCQwIoAginORIhnPEFZUhXm6eWBkm7K7Zc8R4/z7LW4H" +
     * "ossDlCswCgYIKoZIzj0EAwIDRwAwRAIgVikIUZzgfuFsGLQHWJUVJCU7pDaETkaz" +
     * "PzFgsCiLxUACICgzJYlW7nvZxP7b6tbeu3t8mrhMXQs956mD4+BoKuNI";
     */

    public static String privKey = "MD4CAQAwEAYHKoZIzj0CAQYFK4EEAAoEJzAlAgEBBCBnhIob4JXH+WpaNiL72BlbtUMAIBQoM852d+tKFBb7fg==";
    public static String pubKey = "MFYwEAYHKoZIzj0CAQYFK4EEAAoDQgAEavNEKGRcmB7u49alxowlwCi1s24ANOpOQ9UiFBxgqnO/RfOl3BJm0qE2IJgCnvL7XUetwj5C/8MnMWi9ux2aeQ==";

    @SuppressWarnings("static-access")
    public static void main(String[] args) throws IOException {
        if (args.length < 8) {
            System.out.println(
                    "Usage: ... ThroughputLatencyClient <initial client id> <number of clients> <number of operations> <request size> <interval (ms)> <read only?> <verbose?> <nosig | default | ecdsa>");
            System.exit(-1);
        }

        initId = Integer.parseInt(args[0]);
        latencies = new LinkedBlockingQueue<>();
        writerThread = new Thread() {

            public void run() {

                FileWriter f = null;
                try {
                    f = new FileWriter("./latencies_" + initId + ".txt");
                    while (true) {
                        f.write(latencies.take());
                        f.flush();
                    }

                } catch (IOException | InterruptedException ex) {
                    ex.printStackTrace();
                } finally {
                    try {
                        f.close();
                    } catch (IOException ex) {
                        ex.printStackTrace();
                    }
                }
            }
        };
        writerThread.start();

        int numThreads = Integer.parseInt(args[1]);

        int numberOfOps = Integer.parseInt(args[2]);
        int requestSize = Integer.parseInt(args[3]);
        int interval = Integer.parseInt(args[4]);
        boolean readOnly = Boolean.parseBoolean(args[5]);
        boolean verbose = Boolean.parseBoolean(args[6]);
        String sign = args[7];

        int s = 0;
        if (!sign.equalsIgnoreCase("nosig"))
            s++;
        if (sign.equalsIgnoreCase("ecdsa"))
            s++;

        if (s == 2 && Security.getProvider("SunEC") == null) {

            System.out.println("Option 'ecdsa' requires SunEC provider to be available.");
            System.exit(0);
        }

        Client[] clients = new Client[numThreads];

        for (int i = 0; i < numThreads; i++) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException ex) {

                ex.printStackTrace();
            }

            System.out.println("Launching client " + (initId + i));
            clients[i] = new ThroughputLatencyClientICGDynamic.Client(initId + i, numberOfOps, requestSize, interval,
                    readOnly, verbose, s);
        }

        ExecutorService exec = Executors.newFixedThreadPool(clients.length);
        Collection<Future<?>> tasks = new LinkedList<>();

        for (Client c : clients) {
            tasks.add(exec.submit(c));
        }

        // wait for tasks completion
        for (Future<?> currTask : tasks) {
            try {
                currTask.get();
            } catch (InterruptedException | ExecutionException ex) {

                ex.printStackTrace();
            }

        }

        exec.shutdown();

        System.out.println("All clients done.");
    }

    static class Client extends Thread {

        int id;
        int numberOfOps;
        int requestSize;
        int interval;
        boolean readOnly;
        boolean verbose;
        AsynchServiceProxy proxy;
        byte[] request;
        int rampup = 1000;

        public Client(int id, int numberOfOps, int requestSize, int interval, boolean readOnly, boolean verbose,
                int sign) {
            super("Client " + id);

            this.id = id;
            this.numberOfOps = numberOfOps;
            this.requestSize = requestSize;
            this.interval = interval;
            this.readOnly = readOnly;
            this.verbose = verbose;
            this.proxy = new AsynchServiceProxy(id);
            this.request = new byte[this.requestSize];

            Random rand = new Random(System.nanoTime() + this.id);
            rand.nextBytes(request);

            byte[] signature = new byte[0];
            Signature eng;

            try {

                if (sign > 0) {

                    if (sign == 1) {
                        eng = TOMUtil.getSigEngine();
                        eng.initSign(proxy.getViewManager().getStaticConf().getPrivateKey());
                    } else {

                        eng = Signature.getInstance("SHA256withECDSA", "BC");

                        // KeyFactory kf = KeyFactory.getInstance("EC", "BC");
                        // Base64.Decoder b64 = Base64.getDecoder();
                        // PKCS8EncodedKeySpec spec = new
                        // PKCS8EncodedKeySpec(b64.decode(ThroughputLatencyClient.privKey));
                        // eng.initSign(kf.generatePrivate(spec));
                        KeyFactory keyFactory = KeyFactory.getInstance("EC");
                        EncodedKeySpec privateKeySpec = new PKCS8EncodedKeySpec(
                                org.apache.commons.codec.binary.Base64.decodeBase64(privKey));
                        PrivateKey privateKey = keyFactory.generatePrivate(privateKeySpec);
                        eng.initSign(privateKey);

                    }
                    eng.update(request);
                    signature = eng.sign();
                }

                ByteBuffer buffer = ByteBuffer.allocate(request.length + signature.length + (Integer.BYTES * 2));
                buffer.putInt(request.length);
                buffer.put(request);
                buffer.putInt(signature.length);
                buffer.put(signature);
                this.request = buffer.array();

            } catch (NoSuchAlgorithmException | SignatureException | NoSuchProviderException | InvalidKeyException
                    | InvalidKeySpecException ex) {
                ex.printStackTrace();
                System.exit(0);
            }

        }

        public void run() {

            System.out.println("Warm up...");

            int req = 0;

            long initial_time = System.nanoTime();

            for (int i = 0; i < numberOfOps / 2; i++, req++) {
                if (verbose)
                    System.out.print("Sending req " + req + "...");

                // long last_send_instant = System.currentTimeMillis();

                byte[] reply = null;
                if (readOnly)
                    reply = proxy.invokeUnordered(request);
                else
                    reply = proxy.invokeOrdered(request);
                // long latency = System.currentTimeMillis() - last_send_instant;

                // try {
                // if (reply != null)
                // latencies.put(id + "\t" + System.currentTimeMillis() + "\t" + latency +
                // "\n");
                // } catch (InterruptedException ex) {
                // ex.printStackTrace();
                // }

                if (verbose)
                    System.out.println(" sent!");

                if (verbose && (req % 1000 == 0))
                    System.out.println(this.id + " // " + req + " operations sent!");

                try {

                    // sleeps interval ms before sending next request
                    // if (interval > 0) {
                    // Thread.sleep(interval);
                    // }
                    // if (interval < 0) { // if interval negative, use it as upper limit for a
                    // randomized interval
                    // try { // so wait between 0ms and interval ms
                    // double waitTime = Math.random() * interval * -1;
                    // System.out.println("waiting for " + waitTime + " ms");
                    // Thread.sleep(Math.round(waitTime));
                    // } catch (InterruptedException ex) {
                    // }
                    // }

                    if (this.rampup > 0) {
                        Thread.sleep(this.rampup);
                    }
                    this.rampup -= 100;

                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
            }

            try {
                System.out.println("Warm up complete after " + (System.nanoTime() - initial_time) / 1000000000
                        + " s\nWaiting 10s...");
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            // FileWriter f = null;
            // try {
            // f = new FileWriter("./latencies_" + initId + ".txt");
            // } catch (IOException e) {
            // e.printStackTrace();
            // }

            Storage nonest = new Storage(numberOfOps / 2);
            Storage weakst = new Storage(numberOfOps / 2);
            Storage strongst = new Storage(numberOfOps / 2);
            Storage finalst = new Storage(numberOfOps / 2);

            long ex_initial_time = System.nanoTime();

            System.out.println("Executing experiment for " + numberOfOps / 2 + " ops after "
                    + (System.nanoTime() - initial_time) + " us of execution");

            for (int i = 0; i < numberOfOps / 2; i++, req++) {
                long last_send_instant = System.nanoTime();
                if (verbose)
                    System.out.print(this.id + " // Sending req " + req + "...");

                if (readOnly) {
                    proxy.invokeUnordered(request);
                    long latency = System.nanoTime() - last_send_instant;
                    try {
                        latencies.put(
                                id + "\t" + System.nanoTime() + "\t" + latency + " us\n");
                    } catch (InterruptedException ex) {
                        ex.printStackTrace();
                    }
                } else {
                    CorrectableSimple cor = proxy.invokeCorrectable(request);
                    cor.getValueNoneConsistency();
                    long nonelatency = System.nanoTime() - last_send_instant;
                    long nonetime = System.nanoTime() / 1000000000;
                    nonest.store(nonelatency);

                    cor.getValueWeakConsistency();
                    long weaklatency = System.nanoTime() - last_send_instant;
                    long weaktime = System.nanoTime() / 1000000000;
                    weakst.store(weaklatency);

                    cor.getValueLineConsistency();
                    long linelatency = System.nanoTime() - last_send_instant;
                    long linetime = System.nanoTime() / 1000000000;
                    strongst.store(linelatency);

                    cor.getValueFinalConsistency();
                    long finallatency = System.nanoTime() - last_send_instant;
                    long finaltime = System.nanoTime() / 1000000000;
                    finalst.store(finallatency);

                    try {
                        latencies.put(id + "\tt: " + nonetime + " s : " + nonelatency / 1000000 + " ms -> None\n");
                        latencies.put(id + "\tt: " + weaktime + " s : " + weaklatency / 1000000 + " ms -> Weak\n");
                        latencies.put(id + "\tt: " + linetime + " s : " + linelatency / 1000000 + " ms -> Strong\n");
                        latencies.put(id + "\tt: " + finaltime + " s : " + finallatency / 1000000 + " ms -> Final\n");
                        // f.write(id + "\t" + System.nanoTime() + "\t" + nonelatency + " us -> None\n"
                        // + id + "\t" + System.nanoTime() + "\t" + weaklatency + " us -> Weak\n"
                        // + id + "\t" + System.nanoTime() + "\t" + linelatency + " us -> Strong\n"
                        // + id + "\t" + System.nanoTime() + "\t" + finallatency + " us -> Final\n");
                    } catch (InterruptedException ex) {
                        ex.printStackTrace();
                    } // catch (IOException e) {
                      // // TODO Auto-generated catch block
                      // e.printStackTrace();
                      // }
                }

                if (verbose)
                    System.out.println(this.id + " // sent!");
                try {
                    // sleeps interval ms before sending next request
                    if (interval > 0) {

                        Thread.sleep(interval);
                    }
                    if (interval < 0) { // if interval negative, use it as upper limit for a randomized interval

                        double waitTime = Math.random() * interval * -1;
                        System.out.println("waiting for " + waitTime + " ms");
                        Thread.sleep(Math.round(waitTime));
                    }
                    if (this.rampup > 0) {
                        Thread.sleep(this.rampup);
                    }
                    this.rampup -= 100;

                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }

                if (verbose && (req % 1000 == 0))
                    System.out.println(this.id + " // " + req + " operations sent!");
            }

            if (id == initId) {
                System.out.println(this.id + " // Average time for " + numberOfOps / 2 + " executions (-10%) = "
                        + nonest.getAverage(true) / 1000000 + " ms (None consistency)");
                System.out.println(this.id + " // Standard desviation for " + numberOfOps / 2 + " executions (-10%) = "
                        + nonest.getDP(true) / 1000000 + " ms (None consistency)");
                System.out.println(this.id + " // Average time for " + numberOfOps / 2 + " executions (all samples) = "
                        + nonest.getAverage(false) / 1000000 + " ms (None consistency)");
                System.out.println(this.id + " // Standard desviation for " + numberOfOps / 2
                        + " executions (all samples) = " + nonest.getDP(false) / 1000000 + " ms (None consistency)");
                System.out.println(this.id + " // Maximum time for " + numberOfOps / 2 + " executions (all samples) = "
                        + nonest.getMax(false) / 1000000 + " ms (None consistency)");

                System.out.println(this.id + " // Average time for " + numberOfOps / 2 + " executions (-10%) = "
                        + weakst.getAverage(true) / 1000000 + " ms (Weak consistency)");
                System.out.println(this.id + " // Standard desviation for " + numberOfOps / 2 + " executions (-10%) = "
                        + weakst.getDP(true) / 1000000 + " ms (Weak consistency)");
                System.out.println(this.id + " // Average time for " + numberOfOps / 2 + " executions (all samples) = "
                        + weakst.getAverage(false) / 1000000 + " ms (Weak consistency)");
                System.out.println(this.id + " // Standard desviation for " + numberOfOps / 2
                        + " executions (all samples) = " + weakst.getDP(false) / 1000000 + " ms (Weak consistency)");
                System.out.println(this.id + " // Maximum time for " + numberOfOps / 2 + " executions (all samples) = "
                        + weakst.getMax(false) / 1000000 + " ms (Weak consistency)");

                System.out.println(this.id + " // Average time for " + numberOfOps / 2 + " executions (-10%) = "
                        + strongst.getAverage(true) / 1000000 + " ms (Strong consistency)");
                System.out.println(this.id + " // Standard desviation for " + numberOfOps / 2 + " executions (-10%) = "
                        + strongst.getDP(true) / 1000000 + " ms (Strong consistency)");
                System.out.println(this.id + " // Average time for " + numberOfOps / 2 + " executions (all samples) = "
                        + strongst.getAverage(false) / 1000000 + " ms (Strong consistency)");
                System.out.println(
                        this.id + " // Standard desviation for " + numberOfOps / 2 + " executions (all samples) = "
                                + strongst.getDP(false) / 1000000 + " ms (Strong consistency)");
                System.out.println(this.id + " // Maximum time for " + numberOfOps / 2 + " executions (all samples) = "
                        + strongst.getMax(false) / 1000000 + " ms (Strong consistency)");

                System.out.println(this.id + " // Average time for " + numberOfOps / 2 + " executions (-10%) = "
                        + finalst.getAverage(true) / 1000000 + " ms (Final consistency)");
                System.out.println(this.id + " // Standard desviation for " + numberOfOps / 2 + " executions (-10%) = "
                        + finalst.getDP(true) / 1000000 + " ms (Final consistency)");
                System.out.println(this.id + " // Average time for " + numberOfOps / 2 + " executions (all samples) = "
                        + finalst.getAverage(false) / 1000000 + " ms (Final consistency)");
                System.out.println(this.id + " // Standard desviation for " + numberOfOps / 2
                        + " executions (all samples) = " + finalst.getDP(false) / 1000000 + " ms (Final consistency)");
                System.out.println(this.id + " // Maximum time for " + numberOfOps / 2 + " executions (all samples) = "
                        + finalst.getMax(false) / 1000000 + " ms (Final consistency)");
            }

            System.out.println("Finished!");

            proxy.close();
        }
    }
}