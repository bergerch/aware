package bftsmart.forensic;

import java.io.Serializable;
import java.util.Base64;
import java.util.HashMap;
import java.util.Set;

import bftsmart.consensus.messages.ConsensusMessage;
import bftsmart.consensus.messages.MessageFactory;

public class Aggregate implements Serializable {

    private int paxos_type;
    private int consensus_id;

    private HashMap<Integer, Integer> epochMap; // map of senders to epoch (used to validate proof)
    private HashMap<Integer, Object> agg; // map of senders to proof
    private byte[] value; // this is the proposed value by the Quorum (this will be the same for every replica in the quorum)

    public Aggregate(Set<ConsensusMessage> cm) {
        this.agg = new HashMap<>();
        this.epochMap = new HashMap<>();
        for (ConsensusMessage message: cm) {
            agg.put(message.getSender(), message.getProof());
            epochMap.put(message.getSender(), message.getEpoch());
        }
        ConsensusMessage first = cm.iterator().next();
        this.paxos_type = first.getType();
        this.consensus_id = first.getNumber();
        this.value = first.getValue();
    }

    public Set<Integer> get_senders() {
        return agg.keySet();
    }

    public HashMap<Integer, Object> getProofs() {
        return agg;
    }

    public byte[] getValue(){
        return value;
    }

    public int getType(){
        return paxos_type;
    }

    public int getConsensusID(){
        return consensus_id;
    }

    public int getEpoch(int sender_id){
        return epochMap.get(sender_id);
    }

    /*************************** DEBUG METHODS *******************************/

    public String toString() {
        StringBuilder builder = new StringBuilder();

        if (this.paxos_type == MessageFactory.WRITE) {
            builder.append("########## WRITE Aggregate ##########\nsenders\tproof\n");
        } else {
            builder.append("########## ACCEPT Aggregate ##########\nsenders\tproof\n");
        }

        for (Integer id : get_senders()) {
            builder.append(id + "\t" + Base64.getEncoder().encodeToString((byte[]) agg.get(id)) + "\n");
        }

        builder.append("value = " + Base64.getEncoder().encodeToString(value));

        return builder.toString();
    }
}
