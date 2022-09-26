package bftsmart.tom.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class PipelineManager {
    public final int maxConsensusesInExec = 3;
    public final int minConsensusesInExec = 0;
    private final int delayBeforeNewConsensusProposeInMillisec = 20;

    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private List<Integer> consensusesInExecution = new ArrayList<>();
    private HashMap<Integer, Long> consensusesInExecStartTimestamp = new HashMap<>();
    private Long timestampOfLastConsensusProposed = 0L;

    public long getAmountOfMillisecondsToWait() {
        long currentTimestamp = System.nanoTime();
        long diffBetweenLastConsProposedAndCurrTimeInMillisec = TimeUnit.MILLISECONDS.convert((currentTimestamp - timestampOfLastConsensusProposed),TimeUnit.NANOSECONDS);
        long diffBetweenCurrTimeAndDelay = delayBeforeNewConsensusProposeInMillisec - diffBetweenLastConsProposedAndCurrTimeInMillisec;
//        logger.debug("Difference between current time and delay: {} ", diffBetweenCurrTimeAndDelay);
        if (diffBetweenCurrTimeAndDelay > 0)
            return diffBetweenCurrTimeAndDelay;
        else return 0;
    }

    public void setLastProposedTimestamp() {
        logger.debug("Last proposed consensus timestamp: {}", timestampOfLastConsensusProposed);
        timestampOfLastConsensusProposed = System.nanoTime();
        logger.debug("Current proposed consensus timestamp: {}", timestampOfLastConsensusProposed);
    }

    public boolean isDelayedBeforeNewConsensusStart() {
        long currTimestamp = System.nanoTime();
        logger.debug("Current timestamp for starting new consensus: {}", currTimestamp);
        if (timestampOfLastConsensusProposed != 0) {
            long differenceInNanoBetweenConsensuses = currTimestamp - timestampOfLastConsensusProposed;
            long differenceInMillisecBetweenConsensuses = TimeUnit.MILLISECONDS.convert(differenceInNanoBetweenConsensuses, TimeUnit.NANOSECONDS);
            logger.debug("Difference In Millisecond Between Consensuses: {}", differenceInMillisecBetweenConsensuses);
            return (differenceInMillisecBetweenConsensuses >= delayBeforeNewConsensusProposeInMillisec) ? true : false;
        } else {
            return true; // no consensuses in exec.
        }
    }

    public List<Integer> getConsensusesInExecutionList() {
        return this.consensusesInExecution;
    }

    private int getLastValueInExec() {
        return consensusesInExecution.isEmpty() ? -1 : this.consensusesInExecution.get(consensusesInExecution.size() - 1);
    }

    public boolean isLessThanMaxConsInExecListAllowed() {
        return this.isAllowedToAddToConsensusInExecList();
    }

    private boolean isAllowedToAddToConsensusInExecList() {
        return this.consensusesInExecution.size() < maxConsensusesInExec ? true : false;
    }

    public void addToConsensusInExecList(int cid) {
        if (!this.consensusesInExecution.contains(cid) && isAllowedToAddToConsensusInExecList()) {
            this.consensusesInExecution.add(cid);
            logger.debug("Adding to consensusesInExecution value " + (cid));
            logger.debug("Current consensusesInExecution : {} ", this.consensusesInExecution.toString());
        } else {
            logger.debug("Value {} already exist in consensusesInExecution list or the list is full. List size {}: ", cid, this.consensusesInExecution.size());
        }
    }

    public void removeFromConsensusInExecList(int cid) {
        if (this.consensusesInExecution.size() > minConsensusesInExec && this.consensusesInExecution.contains(cid)) {
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
