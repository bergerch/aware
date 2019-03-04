package bftsmart.dynwheat.monitoring;

import bftsmart.reconfiguration.ServerViewController;

public class WriteLatencyMonitor extends MessageLatencyMonitor {

    public WriteLatencyMonitor(ServerViewController controller) {
        super(controller);
    }

}
