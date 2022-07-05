package bftsmart.correctable;

import java.util.concurrent.locks.ReentrantLock;

public class CorrectableFacade {

    private Correctable correctable;
    private ReentrantLock lock = new ReentrantLock();

    public CorrectableFacade(Correctable correctable){
        this.correctable = correctable;
    }
    
    public void invokeWeak(){
        Consistency target = Consistency.WEAK;
    }

    public void invokeStrong(){
        Consistency target = Consistency.LINE;
    }

    public void invokeFinal(){
        Consistency target = Consistency.FINAL;
    }
}
