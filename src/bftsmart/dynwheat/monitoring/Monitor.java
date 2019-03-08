package bftsmart.dynwheat.monitoring;

import bftsmart.reconfiguration.ServerViewController;

import java.util.Timer;
import java.util.TimerTask;

/**
 * Singelton pattern. Only one instance of Monitor should be used
 *
 * @author cb
 */
public class Monitor {


    private static Monitor instance;

    private MessageLatencyMonitor proposeLatencyMonitor;
    private MessageLatencyMonitor writeLatencyMonitor;

    private Long[] freshestProposeLatencies;
    private Long[] freshestWriteLatencies;


    private Monitor (ServerViewController svc) {
        this.writeLatencyMonitor = new MessageLatencyMonitor(svc);
        this.proposeLatencyMonitor = new MessageLatencyMonitor(svc);

        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                freshestWriteLatencies = writeLatencyMonitor.create_M();
            }
        }, 10*1000, 10*1000);
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

    public Long[] getFreshestProposeLatencies() {
        return freshestProposeLatencies;
    }

    public Long[] getFreshestWriteLatencies() {
        return freshestWriteLatencies;
    }
}
