package bftsmart.dynwheat.monitoring;

import bftsmart.reconfiguration.ServerViewController;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Singelton pattern. Only one instance of Monitor should be used
 *
 * @author cb
 */
public class Monitor {

    private static final int MONITORING_DELAY = 10 * 1000;
    private static final int MONITORING_PERIOD = 10 * 1000;

    public static final long MISSING_VALUE = 1000000000000000L; // Long does not have an infinity value, but this
    // value is very large for a latency, roughly
    // 10.000 seconds and will be used

    // Singelton
    private static Monitor instance;

    private ServerViewController svc;

    // Stores and computes latency monitoring information
    private MessageLatencyMonitor proposeLatencyMonitor;
    private MessageLatencyMonitor writeLatencyMonitor;

    // A timed synchronizer which will peridically disseminate monitoring, invokes them with total order
    private MonitoringDataSynchronizer monitoringDataSynchronizer;

    // The latencies the current process measures from its own perspective
    private Long[] freshestProposeLatencies;
    private Long[] freshestWriteLatencies;

    // The measured latency matrices which have been disseminated with total order
    // They are the same in all replicas for a defined consensus id, after all TOMMessages within this consensus
    // have been processed.
    private Long[][] m_propose;
    private Long[][] m_write;

    private Monitor(ServerViewController svc) {

        this.svc = svc;

        // Todo; if system size changes, we need to handle this
        int n = svc.getCurrentViewN();

        // Initialize
        this.writeLatencyMonitor = new MessageLatencyMonitor(svc);
        this.proposeLatencyMonitor = new MessageLatencyMonitor(svc);
        this.m_propose = new Long[n][n];
        this.m_write = new Long[n][n];
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < n; j++) {
                m_write[i][j] = MISSING_VALUE;
                m_propose[i][j] = MISSING_VALUE;
            }
        }

        // Start the synchroniser
        this.monitoringDataSynchronizer = new MonitoringDataSynchronizer(svc);

        // Periodically compute point-to-pont latencies
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                // Computes the most recent point-to-point latency using the last 1000 (monitoring window) measurements
                // from consensus rounds
                freshestWriteLatencies = writeLatencyMonitor.create_M();
            }
        }, MONITORING_DELAY, MONITORING_PERIOD);
    }

    /**
     * Use this method to get the monitor
     *
     * @param svc server view controller
     * @return the monitoring instance
     */
    public static Monitor getInstance(ServerViewController svc) {
        if (Monitor.instance == null) {
            Monitor.instance = new Monitor(svc);
        }
        return Monitor.instance;
    }

    public MessageLatencyMonitor getWriteLatencyMonitor() {
        return writeLatencyMonitor;
    }

    public MessageLatencyMonitor getProposeLatencyMonitor() {
        return proposeLatencyMonitor;
    }

    public Long[] getFreshestProposeLatencies() {
        return freshestProposeLatencies;
    }

    public Long[] getFreshestWriteLatencies() {
        return freshestWriteLatencies;
    }

    /**
     * Gets called when a consensus completes and the consensus includes monitoring TOMMessages with measurement information
     *
     * @param sender      a replica reporting its own measurement from its own perspective
     * @param value       a byte array containing the measurements
     * @param consensusID the specified consensus instance
     */
    public void onReceiveMonitoringInformation(int sender, byte[] value, int consensusID) {
        int n = svc.getCurrentViewN();

        try {
            Long[] rvcdLatencies = MonitoringDataSynchronizer.bytesToLong(value);
            m_write[sender] = rvcdLatencies;
        } catch (IOException e) {
            e.printStackTrace();
        }
        // Debugging and testing:
        printM(sanitize(m_write), consensusID, n);
    }


    /**
     * Assume communication link delays are symmetric and use the maximum
     *
     * @param m latency matrix
     * @return sanitized latency matrix
     */
    public Long[][] sanitize(Long[][] m) {
        int n = svc.getCurrentViewN();
        Long[][] m_ast = new Long[n][n];
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < n; j++) {
                m_ast[i][j] = Math.max(m[i][j], m[j][i]);
            }
        }
        return m_ast;
    }

    private static void printM(Long[][] matrix, int consensusID, int n) {
        System.out.println("Sever Latency Matrix for consensus ID " + consensusID);
        System.out.println("----------------------------------------------------------");
        System.out.println("       0       1       2        3        4        ....    ");
        System.out.println("----------------------------------------------------------");
        for (int i = 0; i < n; i++) {
            System.out.print(i + " | ");
            for (int j = 0; j < n; j++) {

                double latency = Math.round((double) matrix[i][j] / 1000.00); // round to precision of micro seconds
                latency = latency / 1000.00; // convert to milliseconds
                if (latency >= 0.00 & latency < 1.0E9)
                    System.out.print("  " + latency + "  ");
                else
                    System.out.print("  silent  ");
            }
            System.out.println();
        }
    }

    public Long[][] getM_propose() {
        return m_propose;
    }

    public Long[][] getM_write() {
        return m_write;
    }
}
