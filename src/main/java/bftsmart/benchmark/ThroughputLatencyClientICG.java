package bftsmart.benchmark;

import bftsmart.correctable.CorrectableSimple;
import bftsmart.tests.recovery.Operation;
import bftsmart.tom.AsynchServiceProxy;
import bftsmart.tom.ServiceProxy;

import java.net.ResponseCache;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

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
		if (args.length != 6) {
			System.out.println("USAGE: bftsmart.benchmark.ThroughputLatencyClient <initial client id> " +
					"<num clients> <number of operations per client> <request size> <isWrite?> <measurement leader?>");
			System.exit(-1);
		}

		initialClientId = Integer.parseInt(args[0]);
		int numClients = Integer.parseInt(args[1]);
		int numOperationsPerClient = Integer.parseInt(args[2]);
		int requestSize = Integer.parseInt(args[3]);
		boolean isWrite = Boolean.parseBoolean(args[4]);
		boolean measurementLeader = Boolean.parseBoolean(args[5]);
		CountDownLatch latch = new CountDownLatch(numClients);
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
			clients[i] = new Client(initialClientId + i,
					numOperationsPerClient, isWrite, measurementLeader, latch);
			clients[i].start();
			Thread.sleep(10);
		}
		new Thread(() -> {
			try {
				latch.await();
				System.out.println("Executing experiment");
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}).start();
	}

	private static class Client extends Thread {
		private final int clientId;
		private final int numOperations;
		private final boolean isWrite;
		private final AsynchServiceProxy asynch_proxy;
		private final CountDownLatch latch;
		private final boolean measurementLeader;

		public Client(int clientId, int numOperations, boolean isWrite, boolean measurementLeader,
				CountDownLatch latch) {
			this.clientId = clientId;
			this.numOperations = numOperations;
			this.isWrite = isWrite;
			this.measurementLeader = measurementLeader;
			this.latch = latch;
			this.asynch_proxy = new AsynchServiceProxy(clientId);
			this.asynch_proxy.setInvokeTimeout(40); // in seconds
		}

		@Override
		public void run() {
			try {
				latch.countDown();
				if (initialClientId == clientId) {
					asynch_proxy.invokeOrdered(serializedWriteRequest);
				}
				for (int i = 0; i < numOperations; i++) {
					long t1, t2, latency;
					byte[] response;
					t1 = System.nanoTime();
					if (isWrite) {
						CorrectableSimple cor = asynch_proxy.invokeCorrectable(serializedWriteRequest);

						// None consistency
						response = cor.getValueNoneConsistency();
						t2 = System.nanoTime();
						latency = t2 - t1;
						if (initialClientId == clientId && measurementLeader) {
							System.out.println("M:NONE: " + latency);
						}
						// Weak Consistency
						System.out.println("Before WEAK");
						response = cor.getValueWeakConsistency();
						System.out.println("After WEAK");
						t2 = System.nanoTime();
						latency = t2 - t1;
						if (initialClientId == clientId && measurementLeader) {
							System.out.println("M:WEAK: " + latency);
						}
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
				}
			} finally {
				asynch_proxy.close();
			}
		}
	}
}
