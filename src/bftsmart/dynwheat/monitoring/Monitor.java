package bftsmart.dynwheat.monitoring;

import bftsmart.reconfiguration.ServerViewController;

/**
 * Singelton pattern. Only one instance of Monitor should be used
 *
 * @author cb
 */
public class Monitor {


    private static Monitor instance;

    private MessageLatencyMonitor proposeLatencyMonitor;
    private MessageLatencyMonitor writeLatencyMonitor;


    private Monitor (ServerViewController svc) {
        this.writeLatencyMonitor = new MessageLatencyMonitor(svc);
        this.proposeLatencyMonitor = new MessageLatencyMonitor(svc);
    }

    /**
     * Use this method to get the monitor
     *
     * @param svc server view controller
     * @return the monitoring instance
     */
    public static Monitor getInstance (ServerViewController svc) {
        if (Monitor.instance == null) {
            Monitor.instance = new Monitor (svc);
        }
        return Monitor.instance;
    }

    public MessageLatencyMonitor getWriteLatencyMonitor() {
        return writeLatencyMonitor;
    }

    public MessageLatencyMonitor getProposeLatencyMonitor() {
        return proposeLatencyMonitor;
    }
}
