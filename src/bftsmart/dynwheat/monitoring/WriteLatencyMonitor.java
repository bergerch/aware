package bftsmart.dynwheat.monitoring;

import bftsmart.reconfiguration.ServerViewController;

/**
 * @author cb
 */
public class WriteLatencyMonitor extends MessageLatencyMonitor {

    public WriteLatencyMonitor(ServerViewController controller) {
        super(controller);
    }

}
