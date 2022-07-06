package bftsmart.correctable;

import bftsmart.communication.client.ReplyListener;
import bftsmart.tom.RequestContext;
import bftsmart.tom.core.messages.TOMMessage;

public class Correctable {

    private CorrectableState state;
    private Consistency consistency;
    private byte[] value;

    private ReplyListener updateListener;
    private ReplyListener finalListener;

    public Correctable(){
        this.state = CorrectableState.UPDATING;
        this.consistency = Consistency.NONE;
    }

    public CorrectableState getState(){
        return state;
    }

    public Consistency getConsistency(){
        return this.consistency;
    }

    public byte[] getValue(){
        return value;
    }

    //setCallbacks
    public void setCallbacks(ReplyListener updateListener, ReplyListener finalListener){
        this.updateListener = updateListener;
        this.finalListener = finalListener;
    }

    public void update(RequestContext context, TOMMessage reply){
        value = reply.getContent();
        updateListener.replyReceived(context, reply);
    }

    public void close(RequestContext context, TOMMessage reply){
        this.state = CorrectableState.FINAL;
        finalListener.replyReceived(context, reply);
    }

    //speculate
}
