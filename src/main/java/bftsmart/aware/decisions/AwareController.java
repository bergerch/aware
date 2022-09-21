package bftsmart.aware.decisions;

import bftsmart.aware.monitoring.Monitor;
import bftsmart.consensus.messages.ConsensusMessage;
import bftsmart.consensus.messages.MessageFactory;
import bftsmart.consensus.roles.Acceptor;
import bftsmart.reconfiguration.ServerViewController;
import bftsmart.reconfiguration.views.View;
import bftsmart.tom.core.ExecutionManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Implements adaptive wide-area replication
 *
 * Computes the best AWARE configuration used for a reconfiguration
 *
 * @author cb
 */
public class AwareController {

    // constants: emperically determined
    public static final int ROUNDS_AMORTIZATION = 10;
    public static final int N_SIZE_TO_USE_HEURISTICS = 10;

    private static AwareController instance;

    private AwareConfiguration currentDW;

    private WeightConfiguration current;

    // We will compute the worst, median and best weight configurations
    private WeightConfiguration worst; // for evaluations
    private WeightConfiguration median; // for evaluations
    private AwareConfiguration best;

    private ServerViewController viewControl;
    private final ExecutionManager executionManager;

    private Simulator simulator;

    public ServerViewController svc;

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private ReentrantLock computationCompletedLock = new ReentrantLock();

    /**
     * Singelton
     *
     * @param svc Server View Controller
     * @param executionManager Execution Manager
     * @return
     */
    public static AwareController getInstance(ServerViewController svc, ExecutionManager executionManager) {
        if (instance == null) {
            instance = new AwareController(svc, executionManager);
            svc.getCurrentView();
            WeightConfiguration current = new WeightConfiguration(svc.getStaticConf().isBFT(), svc);
            instance.setCurrent(current);
            instance.svc = svc;
        }
        return instance;
    }

    private AwareController(ServerViewController viewControl, ExecutionManager executionManager) {
        this.viewControl = viewControl;
        this.executionManager = executionManager;
        this.simulator = new Simulator(viewControl);

        // Debug
        // Periodically outputs current configuration
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                 logger.info("[AwARE] Controller of id=" + svc.getStaticConf().getProcessId()
                         + ": currently using weights " + instance.getCurrent()
                         + ", leader " + executionManager.getCurrentLeader()
                         + ", view " + svc.getCurrentView().getId()
                         + ", last executed consensus " + executionManager.getLastExec()
                         + ", current Delta " + viewControl.getCurrentView().getDelta());
            }
        }, 10*1000, 5*1000);
    }


    /**
     * This is where we start our search for the best weight configuration and protocol leader. We will generate
     * all possible weight configs first, then compute the predicted latency for all of them and for all possible
     * leader variants. Note, that this way, we traverse the entire search space. Since we compute combinations,
     * the search space is factorial in N and needs to be handled with some heuristics in larger systems
     */
    public AwareConfiguration computeBest(View v) {
        Simulator simulator = new Simulator(viewControl);
        Monitor monitor = Monitor.getInstance(viewControl);

        int[] replicaSet =v.getProcesses();
        int n = v.getN();
        int f = v.getF();
        int u = v.isBFT() ? 2 * f : f;
        int delta = v.getDelta();

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
        if (!instance.svc.getStaticConf().isUseDummyPropose()) {
            propose = write;
        }

        int cid = executionManager.getTOMLayer().getLastExec();

        current = v.getWeightConfiguration();
        currentDW = new AwareConfiguration(current, executionManager.getCurrentLeader());
        Long estimate_current = simulator.predictLatency(replicaSet, currentDW.getLeader(),
                currentDW.getWeightConfiguration(), propose, write, n, f, delta, ROUNDS_AMORTIZATION);
        currentDW.setPredictedLatency(estimate_current);

        // For larger systems, use heuristic, e.g, Simulated Annealing
        if (n > N_SIZE_TO_USE_HEURISTICS) {
            return Simulator.simulatedAnnealing(n, f, delta, u, replicaSet, propose, write, cid).best;
        }


        // Generate the search space:
        //      Computes all possible combinates of R_max and R_min distributions
        List<WeightConfiguration> weightConfigs = WeightConfiguration.allPossibleWeightConfigurations(u, replicaSet);

        List<AwareConfiguration> awareConfigurations = new ArrayList<>();
        int leader = executionManager.getCurrentLeader();

        // Generate the search space
        //      determine if leader should be selected or not
        for (WeightConfiguration w : weightConfigs) {
            if (viewControl.getStaticConf().isUseLeaderSelection()) {
                for (int primary : w.getR_max()) {
                    AwareConfiguration dwConfig = new AwareConfiguration(w, primary);
                    awareConfigurations.add(dwConfig);
                }
            } else {
                AwareConfiguration dwConfig = new AwareConfiguration(w, leader);
                awareConfigurations.add(dwConfig);
            }
        }

        // Compute the predictet latencies of all possible configurations using the simulator
        for (AwareConfiguration dwc : awareConfigurations) {
            Long predictedLatency = simulator.predictLatency(replicaSet, dwc.getLeader(), dwc.getWeightConfiguration(),
                    propose, write, n, f, delta, ROUNDS_AMORTIZATION);
            dwc.setPredictedLatency(predictedLatency);
            //logger.info("WeightConfig " + dwc.getWeightConfiguration() + "with leader " + dwc.getLeader() +
            //        " has predicted latency of " + ((double) Math.round(predictedLatency) / 1000) / 1000.00 + " ms");
        }

        // Sort configurations for ascending predicted latency
        awareConfigurations.sort(Comparator.naturalOrder());
        // We compare worst, median and best:
        AwareConfiguration best = awareConfigurations.get(0);
        AwareConfiguration median = awareConfigurations.get(awareConfigurations.size() / 2); // for evaluation
        AwareConfiguration worst = awareConfigurations.get(awareConfigurations.size() - 1); // for evaluation

        // For testing, remove later, make it debug later
        logger.info("the best config is " + best);
        logger.info("the median config is " + median);
        logger.info("the worst config is " + worst);
        logger.info("");
        logger.info("current config is estimated to be " + estimate_current);

        List<AwareConfiguration> bestConfigs = new ArrayList<>();
        for (AwareConfiguration dwc: awareConfigurations) {
            if (dwc.getPredictedLatency() == best.getPredictedLatency()) {
                bestConfigs.add(dwc);
            }
        }

        int currentLeader = this.executionManager.getCurrentLeader();

        best = bestConfigs.get(0);
        for (AwareConfiguration dwc: bestConfigs) {
            if (dwc.getLeader() == currentLeader) {
                best = dwc;
                break;
            }
        }

        return best;
    }



    public void audit(int cid) {
        int me = viewControl.getStaticConf().getProcessId();
        int interval = 50; // TODO change this to get from configuration file
        // if (svc.getStaticConf().isUseDynamicWeights() && cid % 99 == 0 & cid > 0) { // todo hardcoded interval
        if (svc.getStaticConf().isUseDynamicWeights() && cid > 0 && cid % interval == me * (interval/viewControl.getCurrentViewN())) {
            // perform audit periodically
            // System.out.println("Audit in consensus " + cid);
            MessageFactory factory = new MessageFactory(svc.getStaticConf().getProcessId());
            ConsensusMessage cm = factory.createAudit(svc.getCurrentViewId());
            executionManager.getTOMLayer().getCommunication()
                    .getServersConn().send(svc.getCurrentViewOtherAcceptors(), cm, true);
        }
    }

    /**
     * Optimizes weight distribution and leader selection, and threshold
     *
     * @param cid consensus id
     */
    public void optimize(int cid) {

        // Calculate a good configuration for a future reconfiguration
        if (svc.getStaticConf().isUseDynamicWeights() && cid % svc.getStaticConf().getCalculationInterval() == 0 & cid > 0) {
            // threshold-AWARE: check the next faster view if there is one
           View v = (svc.getStaticConf().isAutoSwitching() && svc.getCurrentView().isFastestConfig())
                   ? svc.getCurrentView()
                   : svc.nextFasterConfig();

           // start new Thread to compute the best AWARE config in the background
            Thread computationOfBestConfig = new Thread(){
                public void run() {
                    logger.info("Started computation of best config in background at cid " + cid);
                    long start = System.nanoTime();
                    computationCompletedLock.lock();
                    /* Begin critical section */
                    AwareController awareController = AwareController.getInstance(svc, executionManager);
                    awareController.setBest(awareController.computeBest(v));
                    /* End critical section */
                    computationCompletedLock.unlock();
                    long end = System.nanoTime();
                    logger.info("Computed the best configuration in " + (end-start)/1000000.0 + " ms " );
                }
            };
            computationOfBestConfig.start();
        }

            // Re-calculate best weight distribution after every x consensus
        if (svc.getStaticConf().isUseDynamicWeights()
                && (cid % svc.getStaticConf().getCalculationInterval() ) == svc.getStaticConf().getCalculationDelay()
                && cid >=  svc.getStaticConf().getCalculationInterval() + svc.getStaticConf().getCalculationDelay()) {

            logger.info("Trying to lock, Computation should be completed, at cid" + cid);

            computationCompletedLock.lock();
            /* begin critical section , should only enter here after computation is completed */

            // Threshold-AWARE: Currently: Periodically try to improve the threshold
            boolean thresholdDecrease = false;
            if (svc.getStaticConf().isAutoSwitching()) {
                if (svc.getCurrentView().isFastestConfig()) {
                    logger.info("System cant get any faster");
                } else {
                    logger.info("###### SWITCH #####");
                    //svc.switchToFasterConfig();
                    thresholdDecrease = true;
                }
            }

            AwareController awareController = AwareController.getInstance(svc, executionManager);
            AwareConfiguration current = awareController.getCurrentDW();
            WeightConfiguration currentWeights = current != null ? current.getWeightConfiguration() : getCurrent();

            // load the best AWARE configuration from the pre-computation made earlier
            WeightConfiguration bestWeights = getBest().getWeightConfiguration();
            logger.info("Best received from pre-computation: " + bestWeights);

            logger.info("");
            logger.info("!!! Best: " + best);
            logger.info("");

            if (svc.getStaticConf().isUseDynamicWeights()
                    && ((!currentWeights.equals(bestWeights) && current.getPredictedLatency() > best.getPredictedLatency() * svc.getStaticConf().getOptimizationGoal())
                    || thresholdDecrease)
            ) {

                if (!thresholdDecrease) System.out.println("Opt.: Current config estimated: " + current.getPredictedLatency() + " and targeted is " +best.getPredictedLatency() );
                // The current weight configuration is not the best
                // Deterministically change weights (this decision will be the same in all correct replicas)

                View currentView = svc.getCurrentView();

                View newView;
                if (thresholdDecrease) {
                    View thresholdDecresedView = svc.nextFasterConfig();
                    newView = new View(thresholdDecresedView.getId(), thresholdDecresedView.getProcesses(), thresholdDecresedView.getF(),
                            thresholdDecresedView.getAddresses(), thresholdDecresedView.isBFT(), thresholdDecresedView.getDelta(), bestWeights);
                    logger.info("================== SWITCH to FAST ==================");
                } else {
                    newView = new View(currentView.getId() + 1, currentView.getProcesses(), currentView.getF(),
                            currentView.getAddresses(), currentView.isBFT(), currentView.getDelta(), bestWeights);
                }

                /** Reconfigure the view here to adjust weights (and if possible also decrease threshold) */
                svc.reconfigureTo(newView);
                /** The system now uses the new view */

                AwareController.getInstance(svc, executionManager).setCurrent(bestWeights);
                logger.info("|AWARE|-" + cid + "-[X] Optimization: Weight adjustment, now using " + bestWeights);
            } else {
                // Keep the current configuration
                logger.info("|AWARE|-"+ cid + "-[ ] Optimization: Weight adjustment, no adjustment," +
                        " current weight config is the best weight config");
            }

            if (svc.getStaticConf().isUseLeaderSelection()
                    && executionManager.getCurrentLeader() != best.getLeader()
                    && current.getPredictedLatency() > best.getPredictedLatency() * svc.getStaticConf().getOptimizationGoal()) {

                // The current leader is not the best, change it to the best
                int newLeader =  (best.getLeader()+svc.getCurrentViewN()) % svc.getCurrentViewN();
                executionManager.setNewLeader(newLeader);

                logger.info("-----_> NEW LEADER SET TO " + newLeader);
                if (svc.getStaticConf().getProcessId() == newLeader) {
                    executionManager.getTOMLayer().imAmTheLeader();
                    logger.info("-----_> I AM THE LEADER NOW" + newLeader);
                } else {
                    Acceptor acceptor = executionManager.getAcceptor();

                   // int cidNew = cid+1;
                    //logger.info("" + acceptor.proposeRecvd[newLeader]  + " " + cidNew );
                    //if(acceptor.proposeRecvd[newLeader] != null)
                       // logger.info("!!!! NUMBER" + acceptor.proposeRecvd[newLeader].getNumber());


                    if(acceptor.proposeRecvd[newLeader] != null && acceptor.proposeRecvd[newLeader].getNumber() == cid + 1) {
                        //logger.info("!!!!!!!!" + acceptor.proposeRecvd[newLeader] + " " + acceptor.proposeRecvd[newLeader].getNumber() + " " + cidNew );
                        //logger.info("!!!!!!!! Receiving propose of new leader");
                        acceptor.processMessage(acceptor.proposeRecvd[newLeader]);
                    }
                }

                logger.info("|AWARE|  [X] Optimization: leader selection, new leader is? " + best.getLeader());
            } else { // Keep the current configuration
                logger.info("|AWARE|  [ ] Optimization: leader selection: no leader change," +
                        " current leader is the best leader");
            }
            Monitor.getInstance(viewControl).init(svc.getCurrentViewN());
            /* End critical section */
            computationCompletedLock.unlock();
            logger.info("Optimization code completed");
        }
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

    public AwareConfiguration getBest() {
        return best;
    }

    public void setBest(AwareConfiguration best) {
        this.best = best;
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

    public AwareConfiguration getCurrentDW() {
        return currentDW;
    }

    public void setCurrentDW(AwareConfiguration currentDW) {
        this.currentDW = currentDW;
    }
}
