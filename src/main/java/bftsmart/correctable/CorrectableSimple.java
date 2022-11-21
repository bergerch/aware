package bftsmart.correctable;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import bftsmart.consensus.roles.Acceptor;
import bftsmart.reconfiguration.ClientViewController;
import bftsmart.reconfiguration.views.View;
import bftsmart.tom.RequestContext;
import bftsmart.tom.core.messages.TOMMessage;

public class CorrectableSimple {

    private CorrectableState state;
    private byte[] ret_value;

    private ClientViewController controller;

    private Semaphore block;

    private double votes = 0.0;
    private int responses = 0;

    // For performance issues this value should be a slightly higher
    // than the time for completing one consensus instance
    private int ACQUIRETIMEOUT = 1000;
    private TimeUnit TIMEUNIT = TimeUnit.MILLISECONDS;

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
    }

    public byte[] getValueNoneConsistency() {
        return getValue(Consistency.NONE);
    }

    public byte[] getValueWeakConsistency() {
        return getValue(Consistency.WEAK);
    }

    public byte[] getValueLineConsistency() {
        return getValue(Consistency.LINE);
    }

    private int minResponses(double nVotes, double vmax, int t) {
        // System.out.printf("nVotes = %f; vmax = %f; t = %d\n", nVotes, vmax, t);
        if (nVotes <= vmax) {
            return 1;
        } else if (nVotes <= vmax * 2 * t) {
            return (int) Math.ceil(nVotes / vmax);
        } else {
            return 2 * t + (int) Math.ceil(nVotes - 2 * t * vmax);
        }
    }

    public byte[] getValueFinalConsistency() {
        return getValue(Consistency.FINAL);
    }

    public byte[] getValue(Consistency consistency) {
        // System.out.println("Get Values Consistency: " + consistency);
        boolean acquire = false;

        double needed_votes = -1.0;
        int needed_responses = -1;

        View cmpView = controller.getCurrentView(); // view to compare with current

        while (true) {
            View current_view = controller.getCurrentView();

            // calculation of needed votes and responces should only happen
            // in the first iteration or if the view as changed
            boolean compute = needed_responses == -1;
            boolean sameView = true;
            if (!compute) {
                sameView = cmpView.equals(current_view);
                compute = !sameView;
            }

            if (compute) {
                if (consistency.equals(Consistency.NONE)) {
                    needed_votes = 1.0;
                    needed_responses = 1;
                } else if (consistency.equals(Consistency.FINAL)) {
                    int t = current_view.getF();
                    int T = current_view.getT();
                    int N = current_view.getN();
                    needed_responses = (N + 2 * T - (t + 1)) / 2;
                } else {
                    int t = current_view.getF();
                    double wMax = 1.00 + ((double) current_view.getDelta() / (double) t);
                    double nVotes = 0.0;
                    if (consistency.equals(Consistency.WEAK)) {
                        nVotes = t * wMax + 1.0;
                    } else if (consistency.equals(Consistency.LINE)) {
                        nVotes = 2 * t * wMax + 1.0;
                    }
                    needed_votes = nVotes;
                    needed_responses = minResponses(nVotes, wMax, t);
                }
                if (!sameView) { // if view has changed update the view to be compared
                    cmpView = current_view;
                }
            }

            // System.out.println("Wainting for " + needed_responses + " responses, and " +
            // needed_votes + " votes");

            try {
                // Do i need the timeout?
                // without time out if there is a reconfiguration a deadlock can happen?
                // TODO test this possibily
                acquire = block.tryAcquire(needed_responses, ACQUIRETIMEOUT, TIMEUNIT);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            if (!acquire) {
                continue;
            }

            if (state == CorrectableState.FINAL) {
                block.release(needed_responses);
                return ret_value;
            }

            if (state == CorrectableState.ERROR) {
                block.release(needed_responses);
                return null;
            }
            // System.out.println("votes >= needed_votes && responses >= needed_responses : " + votes + " >= "
            //         + needed_votes + " && " + responses + " >= " + needed_responses);
            if (votes - needed_votes >= Acceptor.THRESHOLD
                    && responses >= needed_responses) {
                block.release(needed_responses);
                return ret_value;
            } else {
                // If we have the correct number of responses but not the needed votes?
                // sleep here (must be a small sleep otherwise can worsen performance)
            }
            if (acquire) {
                block.release(needed_responses);
            }
        }
    }

    public void update(RequestContext context, TOMMessage reply, double votes, int responses) {
        if (!isFinal()) {
            // mutex.lock();
            block.drainPermits();
            this.votes = votes;
            this.responses = responses;
            ret_value = reply.getContent();
            // System.out.println("update " + responses + ": votes = " + votes + ";
            // responses = " + responses);
            checkFinal();

            block.release(responses); // informs that a responce was received
            // System.out.println(" (C:" + responses+") ");
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