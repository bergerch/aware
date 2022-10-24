package bftsmart.forensic;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import bftsmart.consensus.Epoch;
import bftsmart.reconfiguration.ServerViewController;

public class AuditProvider {

    private Auditor auditor;
    private AuditStorage storage;
    private int last_audit = -1;
    private ServerViewController controller;

    private AuditThread[] t;
    private BlockingQueue<AuditStorage> q;

    // consensus id to number of audits executed
    private Map<Integer, Integer> audit_reps = new HashMap<>(100); // TODO receive max size of storage in config file

    public AuditProvider(ServerViewController controller) {
        this.auditor = new Auditor(controller);
        this.storage = new AuditStorage();
        this.controller = controller;
        int nThreads = controller.getStaticConf().getNumberOfAuditThreads();
        this.t = new AuditThread[nThreads];
        q = new LinkedBlockingQueue<>();
        for (int i = 0; i < nThreads; i++) {
            this.t[i] = new AuditThread();
            this.t[i].start();
        }
    }

    public void registerWrite(Epoch epoch, int cid) {
        if (cid < last_audit)
            return;
        epoch.createWriteAggregate();
        storage.addWriteAggregate(cid, epoch.getWriteAggregate());
    }

    public void registerAccept(Epoch epoch, int cid) {
        if (cid < last_audit)
            return;
        epoch.createAcceptAggregate();
        storage.addAcceptAggregate(cid, epoch.getAcceptAggregate());
    }

    public AuditStorage getStorage() {
        return this.storage;
    }

    /**
     * Adds received storage to Thread queue to be process as soon as possible
     * 
     * @param received
     */
    public void receiveStorage(AuditStorage received) {
        q.add(received);
    }

    /**
     * Compares received storage and system storage
     * 
     * @param receivedStorage received audit storage
     * @return true is no conflit was found, false otherwise
     */
    public boolean compareStorages(AuditStorage receivedStorage) {

        long start_time = System.nanoTime();

        int maxCid = receivedStorage.getMaxCID();

        System.out.println("Comparing storage...");

        if (maxCid <= last_audit) {
            System.out.println(" ======= Storage received does not have needed information ======= ");
            return true; // received storage does not have needed information...
        }
        AuditResult result = auditor.audit(this.storage, receivedStorage, last_audit + 1);

        System.out.println("Comparing Storages took " + (System.nanoTime() - start_time) / 1000000 + " ms");

        if (result.conflictFound()) {
            System.out.println("Conflict found");
            return false;
        } else {
            System.out.println("No conflict found");
            int minCid = receivedStorage.getMinCID();
            int i;
            try {
                for (i = minCid; i <= maxCid; i++) { // update information on audits executed
                    int reps = audit_reps.containsKey(i) ? audit_reps.get(i) : 0;
                    audit_reps.put(i, reps + 1);

                    if (audit_reps.get(i) >= 2 * controller.getCurrentView().getT() + 1) { // used for TAWARE // It is  possible to get a null pointer exception here if the map is deleted by other thread
                        if (audit_reps.get(i) >= 2 * controller.getCurrentView().getF() + 1) {
                            last_audit = i; // this is the last cid that for certain is safe
                        }
                    }
                }
            } catch (NullPointerException e) {
                // audit_rep was cleaned
                
            }
            if (last_audit > 0) {
                clean(last_audit); // TODO check if this is correct
            }
            return true;
        }
    }

    public void clean(int cid) {
        System.out.println(" ======= Storage clean until " + cid + " ======= ");
        storage.removeProofsUntil(cid); // garbage collection for unecessary proofs
        if (storage.getSize() < audit_reps.size()) {
            audit_reps.clear(); // probably not the best way to clean this map
            // maybe do a periodically clean up of this map (shouldn't be the responsability of the threads that audit)
        }
    }

    public void clean() {
        System.out.println(" ======= Storage full clean ======= ");
        storage.removeProofsUntil(storage.getMaxCID()); // garbage collection for unecessary proofs
        if (storage.getSize() < audit_reps.size()) {
            audit_reps.clear(); // probably not the best way to clean this map
        }
    }

    private class AuditThread extends Thread {

        @Override
        public void run() {
            while (true) {
                try {
                    AuditStorage received = q.take();
                    compareStorages(received);
                    System.out.println(" ======= Storage compared in Audit Thread " + this.getName() + " ======= ");
                    System.out.println(" ======= Storages remaining for comparing: " + q.size() + " ======= ");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
