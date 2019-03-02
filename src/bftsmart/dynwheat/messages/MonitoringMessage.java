package bftsmart.dynwheat.messages;

import bftsmart.consensus.messages.ConsensusMessage;

/**
 * This class represents a message used for monitoring the consensus protocol.
 */
public class MonitoringMessage extends ConsensusMessage {


    /**
     * Creates a monitoring message. Used by the message factory to create a COLLECT or PROPOSE message
     *
     * @param paxosType DUMMY_PROPOSE, PROPOSE_RESPONSE, or WRITE_RESPONSE
     * @param id        Consensus's ID
     * @param epoch     Epoch timestamp
     * @param from      This should be this process ID
     * @param value     This should be null if its a COLLECT message, or the proposed value if it is a PROPOSE message
     */
    public MonitoringMessage(int paxosType, int id, int epoch, int from, byte[] value) {
        super(paxosType, id, epoch, from, value);
    }

    @Override
    public String getPaxosVerboseType() {
        if (paxosType == MonitoringMessageFactory.DUMMY_PROPOSE)
            return "DUMMY_PROPOSE";
        else if (paxosType == MonitoringMessageFactory.PROPOSE_RESPONSE)
            return "PROPOSE_RESPONSE";
        else if (paxosType == MonitoringMessageFactory.WRITE_RESPONSE)
            return "WRITE_RESPONSE";
        else
            return "";
    }

    @Override
    public String toString() {
        return "type=" + getPaxosVerboseType() + ", consensusID=" + this.getNumber() + ", epoch=" +
                getEpoch() + ", from=" + getSender();
    }

}

