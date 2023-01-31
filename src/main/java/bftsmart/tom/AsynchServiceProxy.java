package bftsmart.tom;

import bftsmart.communication.client.ReplyListener;
import bftsmart.consensus.roles.Acceptor;
import bftsmart.correctable.Consistency;
import bftsmart.correctable.Correctable;
import bftsmart.correctable.CorrectableSimple;
import bftsmart.reconfiguration.ClientViewController;
import bftsmart.reconfiguration.views.View;
import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.core.messages.TOMMessageType;
import bftsmart.tom.util.Extractor;
import bftsmart.tom.util.KeyLoader;
import bftsmart.tom.util.TOMUtil;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is an extension of 'ServiceProxy' that can waits for replies
 * asynchronously.
 *
 */
public class AsynchServiceProxy extends ServiceProxy {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private HashMap<Integer, RequestContext> requestsContext;
    private HashMap<Integer, TOMMessage[]> requestsReplies;
    private HashMap<Integer, Integer> requestsAlias;

    /**
     * Constructor
     *
     * @see bellow
     */

    public AsynchServiceProxy(int processId) {
        this(processId, null);
        init();
    }
    
    /**
     * Constructor
     *
     * @see bellow
     */
    public AsynchServiceProxy(int processId, String configHome) {
        super(processId, configHome);
        init();
    }

    /**
     * Constructor
     *
     * @see bellow
     */
    public AsynchServiceProxy(int processId, String configHome, KeyLoader loader) {
        super(processId, configHome, loader);
        init();
    }

    /**
     * Constructor
     *
     * @param processId       Process id for this client (should be different from
     *                        replicas)
     * @param configHome      Configuration directory for BFT-SMART
     * @param replyComparator Used for comparing replies from different servers
     *                        to extract one returned by f+1
     * @param replyExtractor  Used for extracting the response from the matching
     *                        quorum of replies
     * @param loader          Used to load signature keys from disk
     */
    public AsynchServiceProxy(int processId, String configHome,
            Comparator<byte[]> replyComparator, Extractor replyExtractor, KeyLoader loader) {

        super(processId, configHome, replyComparator, replyExtractor, loader);
        init();
    }

    private void init() {
        requestsContext = new HashMap<>();
        requestsReplies = new HashMap<>();
        requestsAlias = new HashMap<>();
    }

    private View newView(byte[] bytes) {

        Object o = TOMUtil.getObject(bytes);
        return (o != null && o instanceof View ? (View) o : null);
    }

    /**
     * @see bellow
     */
    public int invokeAsynchRequest(byte[] request, ReplyListener replyListener, TOMMessageType reqType) {
        return invokeAsynchRequest(request, super.getViewManager().getCurrentViewProcesses(), replyListener, reqType);
    }

    /**
     * This method asynchronously sends a request to the replicas.
     * 
     * @param request       Request to be sent
     * @param targets       The IDs for the replicas to which to send the request
     * @param replyListener Callback object that handles reception of replies
     * @param reqType       Request type
     * 
     * @return A unique identification for the request
     */
    public int invokeAsynchRequest(byte[] request, int[] targets, ReplyListener replyListener, TOMMessageType reqType) {
        return invokeAsynch(request, targets, replyListener, reqType);
    }

    /**
     * Purges all information associated to the request.
     * This should always be invoked once enough replies are received and processed
     * by the ReplyListener callback.
     * 
     * @param requestId A unique identification for a previously sent request
     */
    public void cleanAsynchRequest(int requestId) {

        Integer id = requestId;

        do {

            requestsContext.remove(id);
            requestsReplies.remove(id);

            id = requestsAlias.remove(id);

        } while (id != null);

    }

    /**
     * This is the method invoked by the client side communication system.
     *
     * @param reply The reply delivered by the client side communication system
     */
    @Override
    public void replyReceived(TOMMessage reply) {
        logger.debug("Asynchronously received reply from " + reply.getSender() + " with sequence number "
                + reply.getSequence() + " and operation ID " + reply.getOperationId());

        try {
            canReceiveLock.lock();

            RequestContext requestContext = requestsContext.get(reply.getOperationId());

            if (requestContext == null) { // it is not a asynchronous request
                super.replyReceived(reply);
                return;
            }

            if (contains(requestContext.getTargets(), reply.getSender())
                    && (reply.getSequence() == requestContext.getReqId())
                    // && (reply.getOperationId() == requestContext.getOperationId())
                    && (reply.getReqType().compareTo(requestContext.getRequestType())) == 0) {

                logger.debug("Deliverying message from " + reply.getSender() + " with sequence number "
                        + reply.getSequence() + " and operation ID " + reply.getOperationId() + " to the listener");

                ReplyListener replyListener = requestContext.getReplyListener();

                View v = null;

                if (replyListener != null) {
                    // if (reply.getViewID() > getViewManager().getCurrentViewId()) { // Deal with a
                    // system reconfiguration

                    if ((v = newView(reply.getContent())) != null
                            && !requestsAlias.containsKey(reply.getOperationId())) { // Deal with a system
                                                                                     // reconfiguration
                        TOMMessage[] replies = requestsReplies.get(reply.getOperationId());

                        int sameContent = 0; // TODO remove
                        double totalVotes = 0.0;
                        int replyQuorum = getReplyQuorum();

                        int pos = getViewManager().getCurrentViewPos(reply.getSender());

                        replies[pos] = reply;

                        for (int i = 0; i < replies.length; i++) {

                            if ((replies[i] != null) && (i != pos || getViewManager().getCurrentViewN() == 1)
                                    && (reply.getReqType() != TOMMessageType.ORDERED_REQUEST
                                            || Arrays.equals(replies[i].getContent(), reply.getContent()))) {
                                sameContent++;
                                totalVotes += getViewManager().getCurrentView().getWeight(i);
                            }
                        }

                        if (sameContent >= Math.ceil((getViewManager().getCurrentViewN() + getViewManager().getCurrentViewF() + 1) / 2.0)
                                //totalVotes >= replyQuorum
                            && v.getId() > getViewManager().getCurrentViewId() ) {

                            logger.info("sameContent: " + sameContent);
                            logger.info("totalVotes: " + totalVotes);
                            logger.info("replyQuorum: " + replyQuorum);

                            reconfigureTo(v);

                            // System.out.println("Goint to reset");
                            requestContext.getReplyListener().reset();

                            Thread t = new Thread() {

                                @Override
                                public void run() {

                                    int id = invokeAsynch(requestContext.getRequest(), requestContext.getTargets(),
                                            requestContext.getReplyListener(), TOMMessageType.ORDERED_REQUEST);

                                    requestsAlias.put(reply.getOperationId(), id);
                                }

                            };

                            t.start();

                        }

                    } else if (!requestsAlias.containsKey(reply.getOperationId())) {
                        requestContext.getReplyListener().replyReceived(requestContext, reply);
                    }
                }
            }
        } catch (Exception ex) {
            logger.error("Error processing received request", ex);
        } finally {
            canReceiveLock.unlock();
        }
    }

    private int invokeAsynch(byte[] request, int[] targets, ReplyListener replyListener, TOMMessageType reqType) {

        logger.debug("Asynchronously sending request to " + Arrays.toString(targets));

        RequestContext requestContext = null;

        canSendLock.lock();

        requestContext = new RequestContext(generateRequestId(reqType), generateOperationId(),
                reqType, targets, System.currentTimeMillis(), replyListener, request);

        try {
            logger.debug("Storing request context for " + requestContext.getOperationId());
            requestsContext.put(requestContext.getOperationId(), requestContext);
            requestsReplies.put(requestContext.getOperationId(),
                    new TOMMessage[super.getViewManager().getCurrentViewN()]);

            sendMessageToTargets(request, requestContext.getReqId(), requestContext.getOperationId(), targets, reqType);

        } finally {
            canSendLock.unlock();
        }

        return requestContext.getOperationId();
    }

    /**
     *
     * @param targets
     * @param senderId
     * @return
     */
    private boolean contains(int[] targets, int senderId) {
        for (int i = 0; i < targets.length; i++) {
            if (targets[i] == senderId) {
                return true;
            }
        }
        return false;
    }

    /**
     * Correctable method
     */

    // public CorrectableSimple invokeCorrectable(byte[] request){
    // CorrectableSimple correctable = new
    // CorrectableSimple(super.getViewManager());

    // return correctable;
    // }

    public CorrectableSimple invokeCorrectable(byte[] request) {
        CorrectableSimple correctable = new CorrectableSimple(super.getViewManager());

        ClientViewController cViewController = super.getViewManager();
        int targets[] = super.getViewManager().getCurrentViewProcesses();
        logger.debug("Asynchronously sending request to " + Arrays.toString(targets));

        RequestContext requestContext = null;
        TOMMessageType reqType = TOMMessageType.ORDERED_REQUEST;
        // System.out.println("REQUEST " + Arrays.toString(request));
        canSendLock.lock();

        requestContext = new RequestContext(generateRequestId(reqType), generateOperationId(),
                reqType, targets, System.currentTimeMillis(), new ReplyListener() {

                    private int responses = 0;
                    private double votes = 0.0;

                    @Override
                    public void reset() {
                        System.out.println("Correctable reset()");
                        responses = 0;
                        votes = 0;
                        correctable.reset();
                    }

                    @Override
                    public void replyReceived(RequestContext context, TOMMessage reply) {

                        responses++;
                        votes += cViewController.getCurrentView().getWeight(reply.getSender());

                        // System.out.print("Responses received so far: " + responses + ";\tTotal votes acumulated: " + votes);
                        // System.out.println("update");
                        correctable.update(context, reply, votes, responses);
                        // System.out.printf("Votes = %f; Responses = %d\n", votes, responses);

                        if (correctable.isFinal()) { // close after last level of consistency (can be lower than FINAL)
                            cleanAsynchRequest(context.getOperationId()); // TODO do I need to define the last level of
                                                                          // desired consistency to be able to define
                                                                          // when to close? What if I want to close
                                                                          // before FINAL consistency??
                        }
                    }
                }, request);

        try {
            logger.debug("Storing request context for " + requestContext.getOperationId());
            requestsContext.put(requestContext.getOperationId(), requestContext);
            requestsReplies.put(requestContext.getOperationId(),
                    new TOMMessage[super.getViewManager().getCurrentViewN()]);

            sendMessageToTargets(request, requestContext.getReqId(), requestContext.getOperationId(), targets, reqType);

        } finally {
            canSendLock.unlock();
        }
        return correctable;
    }

    public Correctable invokeCorrectable(byte[] request, Consistency[] levels) {
        Correctable correctable = new Correctable();

        int targets[] = super.getViewManager().getCurrentViewProcesses();
        logger.debug("Asynchronously sending request to " + Arrays.toString(targets));

        RequestContext requestContext = null;
        TOMMessageType reqType = TOMMessageType.ORDERED_REQUEST;

        canSendLock.lock();

        requestContext = new RequestContext(generateRequestId(reqType), generateOperationId(),
                reqType, targets, System.currentTimeMillis(), new ReplyListener() {

                    int responces = 0;
                    double votes = 0.0;
                    int level_index = 0;

                    @Override
                    public void reset() {
                        responces = 0;
                        votes = 0;
                        level_index = 0;
                    }

                    @Override
                    public void replyReceived(RequestContext context, TOMMessage reply) {
                        responces++;
                        int sender = reply.getSender();
                        votes += getViewManager().getCurrentView().getWeight(sender);

                        View view = getViewManager().getCurrentView();
                        int delta = view.getDelta();
                        int t = view.getF();
                        double Vmax = 1.0 + (double) delta / (double) t;
                        // System.out.println("Vmax = " + Vmax);
                        int N = view.getN();
                        int T = (N - 1) / 3;

                        double q = calculateConsistencyQ(levels[level_index], (double) t, Vmax);

                        if (votes >= q) {
                            if (levels[level_index].equals(Consistency.FINAL)) {
                                int needed_responses = N - t - 1;
                                if (votes >= q && responces >= needed_responses) { // received weights votes and
                                                                                  // confirmations
                                    System.out.println("Received enouch replies and confirmations, executing Update");
                                    correctable.update(context, reply);
                                    level_index++;
                                }
                            } else {
                                System.out.println("Received enouch replies, executing Update");
                                correctable.update(context, reply);
                                level_index++;
                            }
                        }
                        if (level_index >= levels.length) { // close after last level of consistency (can be lower than
                                                            // FINAL)
                            cleanAsynchRequest(context.getOperationId());
                            correctable.close(context, reply);
                        }
                    }
                }, request);

        try {
            logger.debug("Storing request context for " + requestContext.getOperationId());
            requestsContext.put(requestContext.getOperationId(), requestContext);
            requestsReplies.put(requestContext.getOperationId(),
                    new TOMMessage[super.getViewManager().getCurrentViewN()]);

            sendMessageToTargets(request, requestContext.getReqId(), requestContext.getOperationId(), targets, reqType);

        } finally {
            canSendLock.unlock();
        }
        return correctable;
    }


    public long[] invokeCorrectableLatency(byte[] request) {
        return this.invokeCorrectableLatency(request, new Consistency[]{Consistency.NONE, Consistency.WEAK, Consistency.LINE, Consistency.FINAL});
    }
    public long[] invokeCorrectableLatency(byte[] request, Consistency[] levels) {

        long[] latency = new long[levels.length];
        int targets[] = super.getViewManager().getCurrentViewProcesses();
        logger.debug("Asynchronously sending request to " + Arrays.toString(targets));

        RequestContext requestContext = null;
        TOMMessageType reqType = TOMMessageType.ORDERED_REQUEST;

        canSendLock.lock(); /** Critical Section **/
        long start = System.nanoTime();
        requestContext = new RequestContext(generateRequestId(reqType), generateOperationId(), reqType, targets, System.currentTimeMillis(), new ReplyListener() {

            int responces = 0;
            double votes = 0.0;
            int level_index = 0;

            @Override
            public void reset() {
                responces = 0;
                votes = 0;
                level_index = 0;
                long[] latency = new long[levels.length];
            }

            @Override
            public void replyReceived(RequestContext context, TOMMessage reply) {
                responces++;
                View view = getViewManager().getCurrentView();
                int sender = reply.getSender();
                votes += view.getWeight(sender);

                int delta = view.getDelta();
                int t = view.getF();
                double Vmax = 1.0 + (double) delta / (double) t;
                // System.out.println("Vmax = " + Vmax);
                int N = view.getN();
                int T = (N - 1) / 3;

                double q = calculateConsistencyQ(levels[level_index], (double) t, Vmax);

                if (votes >= q) {
                    if (levels[level_index].equals(Consistency.FINAL)) {
                        int needed_responses = N - t - 1;
                        if (responces >= needed_responses) { // received weights votes and confirmations
                            System.out.println("Received enouch replies and confirmations, executing Update");
                            latency[level_index] = System.nanoTime() - start;
                            level_index++;
                        }
                    } else {
                        System.out.println("Received enouch replies, executing Update");
                        latency[level_index] = System.nanoTime() - start;
                        level_index++;
                    }
                }
                if (level_index >= levels.length) { // close after last level of consistency (can be lower than
                    // FINAL)
                    cleanAsynchRequest(context.getOperationId());
                }
            }
        }, request);

        try {
            logger.debug("Storing request context for " + requestContext.getOperationId());
            requestsContext.put(requestContext.getOperationId(), requestContext);
            requestsReplies.put(requestContext.getOperationId(),
                    new TOMMessage[super.getViewManager().getCurrentViewN()]);

            sendMessageToTargets(request, requestContext.getReqId(), requestContext.getOperationId(), targets, reqType);

        } finally {
            canSendLock.unlock();
        }
        return latency;
    }

    private double calculateConsistencyQ(Consistency level, double t, double Vmax) {
        if (level.equals(Consistency.NONE)) {
            return 1.0;
        }
        if (level.equals(Consistency.WEAK)) {
            return t * Vmax + 1.0 + Acceptor.THRESHOLD;

        } else if (level.equals(Consistency.LINE)) {
            return 2.0 * t * Vmax + 1.0 + Acceptor.THRESHOLD;

        } else if (level.equals(Consistency.FINAL)) {
            // need further confirmations more than (N+2T-(t+1)+1)/2 responces
            return 2.0 * t * Vmax + 1.0 + Acceptor.THRESHOLD;
        }
        System.out.println("Consistency problem, could not calculate Quorum votes");
        return 0.0; // should never reach here
    }
}
