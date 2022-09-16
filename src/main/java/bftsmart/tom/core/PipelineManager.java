package bftsmart.tom.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class PipelineManager {
    public final int maxConsensusesInExec = 3;
    public final int minConsensusesInExec = 0;

    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private List<Integer> consensusesInExecution = new ArrayList<>();

    public List<Integer> getConsensusesInExecution() {
        return this.consensusesInExecution;
    }

    public boolean isLessThanMaxConsInExecListAllowed() {
        return this.isAllowedToAddToConsensusInExecList();
    }

    private boolean isAllowedToAddToConsensusInExecList() {
        return this.consensusesInExecution.size()< maxConsensusesInExec ? true : false;
    }

    public void addToConsensusInExecList(int cid) {
        if(!this.consensusesInExecution.contains(cid) && isAllowedToAddToConsensusInExecList()) {
            this.consensusesInExecution.add(cid);
            logger.debug("Adding to consensusesInExecution value " + (cid));
            logger.debug("Current consensusesInExecution : {} ", this.consensusesInExecution.toString());
        } else {
            logger.debug("Value {} already exist in consensusesInExecution list or the list if full: ",cid);
        }
    }

    public void removeFromConsensusInExecList(int cid) {
        if(this.consensusesInExecution.size() > minConsensusesInExec && this.consensusesInExecution.contains(cid)) {
            this.consensusesInExecution.remove(this.consensusesInExecution.indexOf(cid));
            logger.debug("Removing in consensusesInExecution value: {}", cid);
            logger.debug("Current consensusesInExecution : {} ", this.consensusesInExecution.toString());
        } else {
            logger.warn("Cannot remove value {} in consensusesInExecution list because value not in the list.", cid);
        }
    }

    public void cleanUpConsensusesInExec() {
        this.consensusesInExecution = new ArrayList<>();
    }
}
