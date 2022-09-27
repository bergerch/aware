package bftsmart.benchmark;

import bftsmart.correctable.CorrectableSimple;
import bftsmart.tests.recovery.Operation;
import bftsmart.tom.AsynchServiceProxy;
import bftsmart.tom.ServiceProxy;

import java.net.ResponseCache;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;

import bftsmart.tom.util.Storage;
import org.bouncycastle.asn1.ASN1InputStream;

/**
 * @author robin
 */
public class ThroughputLatencyClientICG {
	private static int initialClientId;
	private static byte[] data;
	private static byte[] serializedReadRequest;
	private static byte[] serializedWriteRequest;

	public static void main(String[] args) throws InterruptedException {
		if (args.length < 7) {
			System.out.println("USAGE: bftsmart.benchmark.ThroughputLatencyClient <initial client id> " +
					"<num clients> <number of operations per client> <request size> <interval> <readonly?> <verbose?>");
			System.exit(-1);
		}

		initialClientId = Integer.parseInt(args[0]);
		int numClients = Integer.parseInt(args[1]);
		int numOperationsPerClient = Integer.parseInt(args[2]);
		int requestSize = Integer.parseInt(args[3]);
		int interval = Integer.parseInt(args[4]);
		boolean readonly = Boolean.parseBoolean(args[5]);
		boolean measurementLeader = Boolean.parseBoolean(args[6]);

		Client[] clients = new Client[numClients];
		data = new byte[requestSize];
		for (int i = 0; i < requestSize; i++) {
			data[i] = (byte) i;
		}
		ByteBuffer writeBuffer = ByteBuffer.allocate(1 + Integer.BYTES + requestSize);
		writeBuffer.put((byte) Operation.PUT.ordinal());
		writeBuffer.putInt(requestSize);
		writeBuffer.put(data);
		serializedWriteRequest = writeBuffer.array();

		ByteBuffer readBuffer = ByteBuffer.allocate(1);
		readBuffer.put((byte) Operation.GET.ordinal());
		serializedReadRequest = readBuffer.array();

		for (int i = 0; i < numClients; i++) {
			clients[i] = new Client(initialClientId + i, numOperationsPerClient, readonly, measurementLeader, interval);
			clients[i].start();
			Thread.sleep(500);
		}
	}

	private static class Client extends Thread {
		private final int clientId;
		private final int numOperations;
		private final boolean readonly;
		private final AsynchServiceProxy asynch_proxy;
		private final boolean measurementLeader;

		private final int interval;


		private final Storage none;
		private final Storage weak;
		private final Storage strong;
		private final Storage finalized;

		public Client(int clientId, int numOperations, boolean readonly, boolean measurementLeader, int interval) {
			this.clientId = clientId;
			this.numOperations = numOperations;
			this.readonly = readonly;
			this.measurementLeader = measurementLeader;
			this.asynch_proxy = new AsynchServiceProxy(clientId);
			this.asynch_proxy.setInvokeTimeout(40); // in seconds
			this.interval = interval;

			none = new Storage(numOperations/2);
			weak = new Storage(numOperations/2);
			strong = new Storage(numOperations/2);
			finalized = new Storage(numOperations/2);
		}

		@Override
		public void run() {
			try {
				System.out.println("Starting client ..");

				System.out.println("Sending first request with view " + asynch_proxy.getViewManager().getCurrentView());

				byte[] res = asynch_proxy.invokeOrdered(serializedWriteRequest);

				Thread.sleep(1000);

				System.out.println("my view now is " + asynch_proxy.getViewManager().getCurrentView());


				LinkedList<long[]> latenciesRounds = new LinkedList<>();

				for (int i = 0; i < numOperations; i++) {
					long t1, t2, latency;
					byte[] response;
					t1 = System.nanoTime();
					if (!readonly) {
						System.out.println("ICG Sending request.. using view id: " + asynch_proxy.getViewManager().getCurrentViewId());
						long[] latencies = asynch_proxy.invokeCorrectableLatency(serializedWriteRequest);
						latenciesRounds.add(latencies);
						System.out.println("Ronda: " + i);

//						// None consistency
//						response = cor.getValueNoneConsistency();
//						t2 = System.nanoTime();
//						latency = t2 - t1;
//						if (initialClientId == clientId && measurementLeader) {
//							System.out.println("M:NONE: " + latency);
//							if (i < numOperations / 2)
//								none.store(latency);
//						}
//						// Weak Consistency
//						response = cor.getValueWeakConsistency();
//						t2 = System.nanoTime();
//						latency = t2 - t1;
//						if (initialClientId == clientId && measurementLeader) {
//							System.out.println("M:WEAK: " + latency);
//							if (i < numOperations / 2)
//								weak.store(latency);
//						}
//
//						// Strong Consistency
//						response = cor.getValueLineConsistency();
//						t2 = System.nanoTime();
//						latency = t2 - t1;
//						if (initialClientId == clientId && measurementLeader) {
//							System.out.println("M:STRONG: " + latency);
//							if (i < numOperations / 2)
//								strong.store(latency);
//						}
//
//						// Final Consistency
//						response = cor.getValueFinalConsistency();
//						t2 = System.nanoTime();
//						latency = t2 - t1;
//						if (initialClientId == clientId && measurementLeader) {
//							System.out.println("M:FINAL: " + latency);
//							if (i < numOperations / 2)
//								finalized.store(latency);
//						}
					} else {
						response = asynch_proxy.invokeUnordered(serializedReadRequest);

						t2 = System.nanoTime();
						latency = t2 - t1;
						if (!Arrays.equals(data, response)) {
							throw new IllegalStateException("The response is wrong");
						}
						if (initialClientId == clientId && measurementLeader) {
							System.out.println("M: " + latency);
						}
					}
					if (interval > 0) {
						Thread.sleep(interval);
					}
					if (interval < 0) { // if interval negative, use it as upper limit for a randomized interval

						double waitTime = Math.random() * interval * -1;
						System.out.println("waiting for " + waitTime + " ms");
						Thread.sleep(Math.round(waitTime));

					}
				}
				// Waiting for last replies to arrive..
				System.out.println("Waiting for last results to arrive..");
				Thread.sleep(30000);

				for (long[] latencies: latenciesRounds) {
					none.store(latencies[0]);
					weak.store(latencies[1]);
					strong.store(latencies[2]);
					finalized.store(latencies[3]);
				}
			} catch (InterruptedException e) {
				System.out.println("Interrupted Thread");
				throw new RuntimeException(e);
			} finally {
				asynch_proxy.close();
				System.out.println(this.clientId + " // NONE Average time for " + numOperations / 2 + " executions (-10%) = " + none.getAverage(true) / 1000 + " us ");
				System.out.println(this.clientId + " // WEAK Average time for " + numOperations / 2 + " executions (-10%) = " + weak.getAverage(true) / 1000 + " us ");
				System.out.println(this.clientId + " // STRONG Average time for " + numOperations / 2 + " executions (-10%) = " + strong.getAverage(true) / 1000 + " us ");
				System.out.println(this.clientId + " // FINAL Average time for " + numOperations / 2 + " executions (-10%) = " + finalized.getAverage(true) / 1000 + " us ");

			}
		}
	}
}
