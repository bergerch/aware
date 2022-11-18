package bftsmart.correctable;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import bftsmart.consensus.roles.Acceptor;
import bftsmart.reconfiguration.ClientViewController;
import bftsmart.reconfiguration.views.View;
import bftsmart.tom.RequestContext;
import bftsmart.tom.core.messages.TOMMessage;

public class CorrectableSimple {

    private CorrectableState state;
    private byte[] ret_value;

    private ClientViewController controller;
    
    // private ReentrantLock mutex = new ReentrantLock();
    private Semaphore block;

    private double votes = 0.0;
    private int responses = 0;

    public CorrectableSimple(ClientViewController controller) {
        this.state = CorrectableState.UPDATING;
        this.ret_value = null;

        this.controller = controller;

        this.block = new Semaphore(controller.getCurrentViewN(), true);
        this.block.drainPermits();
    }

    public void reset() {
        this.state = CorrectableState.UPDATING;
        ret_value = null;
        votes = 0.0;
        responses = 0;
        block = new Semaphore(controller.getCurrentViewN());
        try {
            block.acquire(controller.getCurrentViewN());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // mutex = new ReentrantLock();
    }

    public byte[] getValueNoneConsistency() {
        return getValue(1.0, 1);
    }

    public byte[] getValueWeakConsistency() {
        View current_view = controller.getCurrentView();
        int t = current_view.getF();
        double wMax = 1.00 + ((double) current_view.getDelta() / (double) t);
        double nVotes = t * wMax + 1.0;
        return getValue(t * wMax + 1.0, minResponses(nVotes, wMax, t));
    }

    public byte[] getValueLineConsistency() {
        View current_view = controller.getCurrentView();
        int t = current_view.getF();
        double wMax = 1.00 + ((double) current_view.getDelta() / (double) t);
        double nVotes = 2 * t * wMax + 1.0;
        return getValue(2 * t * wMax + 1.0, minResponses(nVotes, wMax, t));
    }

    private int minResponses(double nVotes, double vmax, int t) {
        // System.out.printf("nVotes = %f; vmax = %f; t = %d\n", nVotes, vmax, t);
        if (nVotes <= vmax) {
            return 1;
        } else if(nVotes <= vmax * 2 * t) {
            return (int)Math.ceil(nVotes/vmax);
        } else {
            return 2*t + (int) Math.ceil(nVotes - 2*t*vmax);
        }
    }

    public byte[] getValueFinalConsistency() {
        View current_view = controller.getCurrentView();
        int t = current_view.getF();
        int T = current_view.getT();
        int N = current_view.getN();
        int needed_responses = (N + 2 * T - (t + 1)) / 2;
        // int time = 1;
        while (true) {
            try {
                block.tryAcquire(needed_responses, 100, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            // System.out.printf("after block of %d needed response for the %d time\n", needed_responses, time++);
            if (state == CorrectableState.FINAL) {
                // System.out.printf("Will return final consistency using %d responses and %f votes (needed at least %d responses)\n", responses, votes, needed_responses);
                block.release(needed_responses);
                return ret_value;
            }
            if (state == CorrectableState.ERROR) {
                block.release(needed_responses);
                return null;
            }
            block.release(needed_responses);
        }
    }

    /**
     * Gets decision value after waiting for needed_weights and needed_responces
     * 
     * @param needed_votes
     * @param needed_responses
     * @return decision value if Correct, null otherwise (ERROR state)
     */
    public byte[] getValue(double needed_votes, int needed_responses) {
        // int time = 1;
        while (true) {
             //System.out.println("before try Acquire");
            try {
                block.tryAcquire(needed_responses, 100, TimeUnit.MILLISECONDS); // do i need the timeout?
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            // System.out.println("Before release");
            try {
                Thread.sleep(0, 20);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            //System.out.println("Slept 1 ms");
            // mutex.lock();
            if (state == CorrectableState.ERROR) {
                // mutex.unlock();
                block.release(needed_responses);
                return null;
            }
            // System.out.println("votes >= needed_votes && responses >= needed_responses : " + votes + " >= " + needed_votes + " && " + responses + " >= " + needed_responses);
            if (votes - needed_votes >= Acceptor.THRESHOLD
                    && responses >= needed_responses) {
                // System.out.printf("Will return using %d responses and %f votes (needed at least %d responses and %f votes)\n", responses, votes, needed_responses, needed_votes);
                // mutex.unlock();
                block.release(needed_responses);
                return ret_value;
            }
            // mutex.unlock();
            block.release(needed_responses);
        }
    }

    public void update(RequestContext context, TOMMessage reply, double votes, int responses) {
        if (!isFinal()) {
            // mutex.lock();
            block.drainPermits();
            this.votes = votes;
            this.responses = responses;
            ret_value = reply.getContent();
            // System.out.println("update " + responses + ": votes = " + votes + "; responses = " + responses);
            checkFinal();
            
            block.release(responses); // informs that a responce was received
            // mutex.unlock();
        }
    }

    private void checkFinal() {
        View current_view = controller.getCurrentView();
        int t = current_view.getF();
        int T = current_view.getT();
        int N = current_view.getN();
        double wMax = 1.00 + ((double) current_view.getDelta() / (double) t);
        int needed_responces =  N - t - 1;
        if (this.responses >= needed_responces && this.votes  >= 2 * t * wMax + 1.0 + Acceptor.THRESHOLD) {
            close();
        }
    }

    public void close() {
        this.state = CorrectableState.FINAL;
    }

    public boolean isFinal() {
        return this.state == CorrectableState.FINAL;
    }

}