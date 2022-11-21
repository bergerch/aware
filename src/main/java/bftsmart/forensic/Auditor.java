package bftsmart.forensic;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import bftsmart.consensus.messages.ConsensusMessage;
import bftsmart.reconfiguration.ServerViewController;
import bftsmart.tom.util.TOMUtil;

/**
 * Class responsible for receiving audit storages and check for conflict
 */
public class Auditor {

    private boolean verbose = false;
    private ServerViewController controller;

    public Auditor(ServerViewController controller) {
        this.controller = controller;
    }

    /**
     * Checks for conflict between two storages
     * 
     * @param local_storage    local storage
     * @param received_storage received storage
     * @param minCid           lowest consensus id needed to be checked
     * @return audit result with conflicts if found
     */
    public AuditResult audit(AuditStorage local_storage, AuditStorage received_storage, int minCid) {
        AuditResult result = new AuditResult();

        checkConflict(local_storage.getWriteAggregate(), received_storage.getWriteAggregate(), minCid, result);
        checkConflict(local_storage.getWriteAggregate(), received_storage.getAcceptAggregate(), minCid, result);
        checkConflict(local_storage.getAcceptAggregate(), received_storage.getAcceptAggregate(), minCid, result);

        return result;
    }

    private void checkConflict(Map<Integer, Aggregate> local, Map<Integer, Aggregate> received, int minCid,
            AuditResult result) {

        Set<Integer> set = local.keySet();
        for (Integer c_id : set) {
            try {
                if (minCid > c_id || received.get(c_id) == null) {
                    continue;
                }
                boolean isValid = validSignature(received.get(c_id));// validate received aggregate signature
                if (!isValid) {
                    // This aggregate is faulty (was received with bad signature)
                    // sender of storage should be considered faulty
                    // testing should end here?
                    // result.invalidSignatureFound();
                    // System.out.println("Aggregate Signatures Invalid!!");
                    // return; //stop forensics? Since we received an invalid signature, should we
                    // ignore the correct ones from this sender?
                    continue; // even if signature is incorrect ignore and continue to next record
                }
                if (!Arrays.equals(local.get(c_id).getValue(), received.get(c_id).getValue())) { // null can happen here
                                                                                                 // if local storage was
                                                                                                 // clean between audits
                    // If values for the same consensus id are not equal, conflict has happen
                    System.out.println(String.format("Aggreates of consensus id %d have conflict", c_id));
                    if (c_id < result.getFaultyView()) {
                        result.setFaultyView(c_id);
                    }

                    Set<Integer> senders = local.get(c_id).get_senders();
                    for (Integer sender_id : received.get(c_id).get_senders()) {
                        if (senders.contains(sender_id))
                            result.addReplica(sender_id); // a replica chose more than one value
                    }
                    System.out.println("Faulty replicas so far: " + Arrays.toString(result.getReplicasArray()));
                } else {
                    // System.out.println(String.format("Aggreates of consensus id %d have no
                    // conflict", c_id));
                }
            } catch (NullPointerException e) {
                return;
            }
        }
    }

    /**
     * Checks for conflict between two storages
     * 
     * @param local_storage    local storage
     * @param received_storage received storage
     * @return audit result with conflicts if found
     */
    public AuditResult audit(AuditStorage local_storage, AuditStorage received_storage) {
        return audit(local_storage, received_storage, 0);
    }

    /**
     * This method checks is an Aggregate has valid signatures
     * If a signature does not decrypt to the values saved in the Aggregate
     * the aggregate is invalid
     * 
     * @param agg Aggregate to check
     * @return true if Aggregate proofs corresponds to the correct value, false
     *         otherwise
     */
    public boolean validSignature(Aggregate agg) {
        // System.out.println("Validating signatures");

        int type = agg.getType();
        int consensus_id = agg.getConsensusID();
        byte[] value = agg.getValue();

        for (Integer sender_id : agg.get_senders()) {

            byte[] proof = (byte[]) agg.getProofs().get(sender_id);
            ConsensusMessage dummi = new ConsensusMessage(type, consensus_id, agg.getEpoch(sender_id), sender_id,
                    value);

            byte[] data = TOMUtil.getBytes(dummi);

            boolean valid = TOMUtil.verifySignature(controller.getStaticConf().getPublicKey(sender_id), data, proof);

            if (!valid) {
                System.out.println(String.format("signature is incorrect from sender %d in consensus %d", sender_id,
                        consensus_id));
                return false;
            } else {
                // System.out.println("signature is correct");
            }
        }
        return true;
    }

    private byte[] makeProof(ConsensusMessage cm) {
        ByteArrayOutputStream bOut = new ByteArrayOutputStream(248);
        try {
            ObjectOutputStream obj = new ObjectOutputStream(bOut);
            obj.writeObject(cm);
            obj.flush();
            bOut.flush();
        } catch (IOException ex) {

        }

        byte[] data = bOut.toByteArray();

        // Always sign a consensus proof.
        return TOMUtil.signMessage(controller.getStaticConf().getPrivateKey(), data);
        // System.out.println("signature:\n" + Arrays.toString(signature));
    }
}
