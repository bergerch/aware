package bftsmart.correctable;

import java.rmi.RemoteException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Phaser;
import java.util.concurrent.locks.ReentrantLock;

import bftsmart.reconfiguration.ClientViewController;
import bftsmart.reconfiguration.views.View;
import bftsmart.tom.RequestContext;
import bftsmart.tom.core.messages.TOMMessage;

public class CorrectableSimple {

    private CorrectableState state;
    private byte[] ret_value;

    private Map<byte[], Double> vote_map; // value to weights receives
    private Map<byte[], Integer> response_map; // value to responces received

    private ClientViewController controller;

    private ReentrantLock mutex = new ReentrantLock();
    private Phaser phaser;

    public CorrectableSimple(ClientViewController controller) {
        this.state = CorrectableState.UPDATING;
        this.ret_value = null;

        vote_map = new HashMap<>();
        response_map = new HashMap<>();

        this.controller = controller;

        this.phaser = new Phaser(controller.getCurrentViewN());
    }

    public void reset() {
        this.state = CorrectableState.UPDATING;
        ret_value = null;

        vote_map = new HashMap<>();
        response_map = new HashMap<>();
    }

    public byte[] getValueNoneConsistency() {
        return getValue(1.0, 1);
    }

    public byte[] getValueWeakConsistency() {
        View current_view = controller.getCurrentView();
        System.out.println(current_view);
        int t = current_view.getF();
        double wMax = 1.00 + ((double) current_view.getDelta() / (double) t);
        return getValue(t * wMax + 1.0, 1);
    }

    public byte[] getValueLineConsistency() {
        View current_view = controller.getCurrentView();
        int t = current_view.getF();
        double wMax = 1.00 + ((double) current_view.getDelta() / (double) t);
        return getValue(2 * t * wMax + 1.0, 1);
    }

    public byte[] getValueFinalConsistency() {
        while (true) {
            if (state == CorrectableState.FINAL) {
                return ret_value;
            }
            if (state == CorrectableState.ERROR) {
                return null;
            }
            // lock.lock();
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
        while (true) {

            if (state == CorrectableState.FINAL) {
                return ret_value;
            }
            if (state == CorrectableState.ERROR) {
                return null;
            }
            mutex.lock();
            // System.out.println("needed = " + needed_votes + ", have = " + vote_map.get(ret_value));
            // System.out.println("needed = " + needed_responses + ", have = " + response_map.get(ret_value));
            // System.out.println(isFinal());
            if (ret_value != null && vote_map.get(ret_value) >= needed_votes
                    && response_map.get(ret_value) >= needed_responses) {
                mutex.unlock();
                return ret_value;
            }
            mutex.unlock();
        }
    }

    public void update(RequestContext context, TOMMessage reply) {

        System.out.println("Correctable update ");

        int sender = reply.getSender();
        double sender_weight = controller.getCurrentView().getWeight(sender);

        mutex.lock();

        byte[] content = reply.getContent();
        System.out.println("content = " + Arrays.toString(content));
        System.out.println("Hash = " + content.hashCode());

        Integer current_responces = response_map.get(content);
        Double current_weight = vote_map.get(content);

        if (current_responces == null)
            current_responces = 0;
        if (current_weight == null)
            current_weight = 0.0;

        response_map.put(content, current_responces + 1);
        vote_map.put(content, current_weight + sender_weight);

        System.out.println(vote_map);

        // TODO check for ERROR
        if (ret_value == null)
            ret_value = content;
        else if (!content.equals(ret_value) && vote_map.get(content) > vote_map.get(ret_value)) { // if new value has
                                                                                                  // more weight change
                                                                                                  // correctable value
            ret_value = content;
        }

        // check if it should be closed
        checkFinal(content);
        // lock.unlock(); // unlock possible responses to clients.
        mutex.unlock();
    }

    private void checkFinal(byte[] value) { // TODO can I otimize this?
        View current_view = controller.getCurrentView();
        int t = current_view.getF();
        int T = current_view.getT();
        int N = current_view.getN();
        double wMax = 1.00 + ((double) current_view.getDelta() / (double) t);
        int responces = (N + 2 * T - (t + 1)) / 2;
        if (vote_map.get(value) >= 2 * t * wMax + 1.0 && response_map.get(value) >= responces) {
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
