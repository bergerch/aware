package bftsmart.dynwheat.decisions;

import bftsmart.dynwheat.monitoring.Monitor;
import bftsmart.reconfiguration.ServerViewController;
import bftsmart.tom.core.ExecutionManager;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Computes the best DynWHEAT configuration used for a reconfiguration
 *
 * @author cb
 */
public class WeightController {

    private static WeightController instance;


    private WeightConfiguration current;

    // We will compute the worst, median and best weight configurations
    private WeightConfiguration worst; // for evaluations
    private WeightConfiguration median; // for evaluations
    private WeightConfiguration optimum;

    private ServerViewController viewControl;
    private ExecutionManager executionManager;

    private Simulator simulator;

    public ServerViewController svc;


    public static WeightController getInstance(ServerViewController svc, ExecutionManager executionManager) {
        if (instance == null) {
            instance = new WeightController(svc, executionManager);
            WeightConfiguration current = new WeightConfiguration(svc.getStaticConf().isBFT(), svc);
            instance.setCurrent(current);
            instance.svc = svc;
        }
        return instance;
    }


    private WeightController(ServerViewController viewControl, ExecutionManager executionManager) {

        this.viewControl = viewControl;
        this.executionManager = executionManager;
        this.simulator = new Simulator(viewControl);
    }


    /**
     * This is where we start our search for the best weight configuration and protocol leader. We will generate
     * all possible weight configs first, then compute the predicted latency for all of them and for all possible
     * leader variants. Note, that this way, we traverse the entire search space. Since we compute combinations,
     * the search space is factorial in N and needs to be handled with a cut & branch heuristic in larger systems
     */
    public DWConfiguration computeBest() {
        Simulator simulator = new Simulator(viewControl);
        int[] replicaSet = viewControl.getCurrentViewProcesses();
        Monitor monitor = Monitor.getInstance(viewControl);

        int n = viewControl.getCurrentViewN();
        int f = viewControl.getCurrentViewF();
        int u = viewControl.getStaticConf().isBFT() ? 2 * f : f;
        int delta = viewControl.getStaticConf().getDelta();

        // init matrices
        long[][] propose = new long[n][n];
        long[][] write = new long[n][n];
        Long[][] propose_ast = monitor.sanitize(monitor.getM_propose());
        Long[][] write_ast = monitor.sanitize(monitor.getM_write());

        // Long to long
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < n; j++) {
                propose[i][j] = propose_ast[i][j];
                write[i][j] = write_ast[i][j];
            }
        }

        // Generate the search space:
        //      Computes all possible combinates of R_max and R_min distributions
        List<WeightConfiguration> weightConfigs = WeightConfiguration.allPossibleWeightConfigurations(u, replicaSet);

        List<DWConfiguration> dwConfigurations = new ArrayList<>();
        int leader = executionManager.getCurrentLeader();

        // Generate the search space
        //      determine if leader should be selected or not
        for (WeightConfiguration w : weightConfigs) {
            if (viewControl.getStaticConf().isUseLeaderSelection()) {
                for (int primary : w.getR_max()) { // todo Only replicas in R_max will be considered to become leader ?
                    DWConfiguration dwConfig = new DWConfiguration(w, primary);
                    dwConfigurations.add(dwConfig);
                }
            } else {
                DWConfiguration dwConfig = new DWConfiguration(w, leader);
                dwConfigurations.add(dwConfig);
            }
        }

        if (!instance.svc.getStaticConf().isUseDummyPropose()) {
            propose = write;
        }

        // Compute the predictet latencies of all possible configurations using the simulator
        for (DWConfiguration dwc : dwConfigurations) {
            Long predictedLatency = simulator.predictLatency(replicaSet, dwc.getLeader(), dwc.getWeightConfiguration(),
                    propose, write, n, f, delta);
            dwc.setPredictedLatency(predictedLatency);
            System.out.println("WeightConfig " + dwc.getWeightConfiguration() + "with leader " + dwc.getLeader() +
                    " has predicted latency of " + ((double) Math.round(predictedLatency) / 1000) / 1000.00 + " ms");
        }

        // Sort configurations for ascending predicted latency
        dwConfigurations.sort(Comparator.naturalOrder());
        // We compare worst, median and best:
        DWConfiguration best = dwConfigurations.get(0);
        DWConfiguration median = dwConfigurations.get(dwConfigurations.size() / 2); // for evaluation
        DWConfiguration worst = dwConfigurations.get(dwConfigurations.size() - 1); // for evaluation

        // For testing, remove later
        System.out.println("the best config is " + best);
        System.out.println("the median config is " + median);
        System.out.println("the worst config is " + worst);

        return best;
    }

    /**
     * Getter and Setter
     **/

    public WeightConfiguration getCurrent() {
        return current;
    }

    public void setCurrent(WeightConfiguration current) {
        this.current = current;
    }

    public WeightConfiguration getWorst() {
        return worst;
    }

    public void setWorst(WeightConfiguration worst) {
        this.worst = worst;
    }

    public WeightConfiguration getMedian() {
        return median;
    }

    public void setMedian(WeightConfiguration median) {
        this.median = median;
    }

    public WeightConfiguration getOptimum() {
        return optimum;
    }

    public void setOptimum(WeightConfiguration optimum) {
        this.optimum = optimum;
    }

    public ServerViewController getViewControl() {
        return viewControl;
    }

    public void setViewControl(ServerViewController viewControl) {
        this.viewControl = viewControl;
    }

    public Simulator getSimulator() {
        return simulator;
    }

    public void setSimulator(Simulator simulator) {
        this.simulator = simulator;
    }
}
