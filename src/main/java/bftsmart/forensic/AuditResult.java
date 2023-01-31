package bftsmart.forensic;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Unnecessary Class
 * Should be removed in the future
 */
public class AuditResult {

    private Set<Integer> replicas;
    private int faultyView;
    private boolean valid_signatures = true;

    public AuditResult(){
        faultyView = Integer.MAX_VALUE;
        replicas = new HashSet<>();
    }

    public AuditResult(Set<Integer> replicas, int faultyView) {
        this.replicas = replicas;
        this.faultyView = faultyView;
    }

    public Set<Integer> getReplicas() {
        return this.replicas;
    }

    public int[] getReplicasArray(){
        int[] array = new int[replicas.size()];
        Iterator<Integer> ite = replicas.iterator();
        for (int i = 0; i < array.length; i++) {
            array[i] = ite.next();
        }
        return array;
    }

    public void addReplica(int id){
        this.replicas.add(id);
    }

    public void setReplicas(Set<Integer> replicas) {
        this.replicas = replicas;
    }

    public int getFaultyView() {
        return this.faultyView;
    }

    public void setFaultyView(int faultyView) {
        this.faultyView = faultyView;
    }

    public boolean conflictFound(){
        return !replicas.isEmpty();
    }

    public boolean validSignatures(){
        return valid_signatures;
    }

    public String toString(){
        return "Falty view: " + faultyView + ", Faulty replicas: " + replicas.toString();
    }

    public void invalidSignatureFound() {
        this.valid_signatures = false;
    }
}
