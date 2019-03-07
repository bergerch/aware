package bftsmart.dynwheat.decisions;

import bftsmart.dynwheat.monitoring.WriteLatencyMonitor;
import bftsmart.reconfiguration.ServerViewController;

public class WeightController {

    private WeightConfiguration current;
    private WeightConfiguration optimum;

    private ServerViewController viewControl;

    private Simulator simulator;

    // TODO LatencySynchronizer: disseminates monitored latencies and collects from other replicas


    public WeightController(ServerViewController viewControl) {

        this.viewControl = viewControl;
        this.simulator = new Simulator(viewControl);

    }
}
