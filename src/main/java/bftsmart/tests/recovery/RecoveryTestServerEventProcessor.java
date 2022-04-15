package bftsmart.tests.recovery;

import master.IProcessingResult;
import pod.IWorkerEventProcessor;

/**
 * @author Robin
 */
public class RecoveryTestServerEventProcessor implements IWorkerEventProcessor {
    private final String readyPattern;
    private boolean isReady;

    public RecoveryTestServerEventProcessor() {
        readyPattern = "Ready to process operations";
    }

    @Override
    public void process(String line) {
        if(!isReady && line.contains(readyPattern))
            isReady = true;
    }

    @Override
    public boolean isReady() {
        return isReady;
    }

    @Override
    public void startProcessing() {
    }

    @Override
    public void stopProcessing() {
    }

    @Override
    public IProcessingResult getProcessingResult() {
        return null;
    }

    @Override
    public boolean ended() {
        return false;
    }

}
