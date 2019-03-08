package bftsmart.dynwheat.monitoring;

import bftsmart.reconfiguration.ServerViewController;

/**
 * This class disseminates this replicas measurements with total order
 * @author cb
 */
public class MonitoringDataSynchronizer {


    private ServerViewController controller;

    public MonitoringDataSynchronizer(ServerViewController svc) {
        this.controller = svc;
    }

}
