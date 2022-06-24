package bftsmart.forensic;

import java.util.HashMap;
import java.util.Map;

import bftsmart.consensus.Epoch;
import bftsmart.reconfiguration.ServerViewController;

public class AuditProvider {

    private Auditor auditor;
    private AuditStorage storage;
    private int last_audit = -1;
    private ServerViewController controller;

    // consensus id to number of audits executed
    private Map<Integer, Integer> audit_reps = new HashMap<>(100); //TODO receive max size of storage in config file

    public AuditProvider(ServerViewController controller){
        this.auditor = new Auditor();
        this.storage = new AuditStorage();
        this.controller = controller;
    }

    public void registerWrite(Epoch epoch, int cid){
        if (cid >= last_audit) return;
        epoch.createWriteAggregate();
        storage.addWriteAggregate(cid, epoch.getWriteAggregate());
    }

    public void registerAccept(Epoch epoch, int cid) {
        if (cid >= last_audit) return;
        epoch.createAcceptAggregate();
        storage.addAcceptAggregate(cid, epoch.getAcceptAggregate());
    }

    public AuditStorage getStorage(){
        return this.storage;
    }

    /**
     * Compares received storage and system storage
     * @param receivedStorage received audit storage
     * @return true is no conflit was founs, false otherwise
     */
    public boolean compareStorages(AuditStorage receivedStorage) {

        int minCid = Math.min(receivedStorage.getMinCID(), last_audit);
		int maxCid = receivedStorage.getMaxCID();

		if (maxCid <= last_audit) {
			return true; // received storage does not have needed information...
		}
		AuditResult result = auditor.audit(this.storage, receivedStorage, last_audit + 1);

		if (result.conflictFound()) {
			System.out.println(result);
			// TODO inform other replicas <PANIC MESSAGE>
			// change config to safer config -> TODO remove faulty from view
			controller.switchToSaferConfig();
			// TODO Rollback
            return false;
		} else {
			// System.out.println("No conflict found");
            for (int i = minCid; i <= maxCid; i++) { // update information on audits executed
                int reps = audit_reps.containsKey(i) ? audit_reps.get(i) : 0;
                audit_reps.put(i, reps + 1);
                
                if (audit_reps.get(i) >= 2 * controller.getCurrentView().getT() + 1) { // need to be done with T not t
                    last_audit = i; // this is the last cid that for certain is safe
                    storage.removeProofsUntil(i); // garbage collection for unecessary proofs
                }
            }
            if (storage.getSize() < audit_reps.size()) {
                audit_reps.clear(); // probably not the best way to clean this map
            }
            return true;
		}
    } 
    
}
