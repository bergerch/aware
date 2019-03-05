package bftsmart.dynwheat.decisions;

import bftsmart.reconfiguration.ServerViewController;
import bftsmart.reconfiguration.views.View;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

public class WeightConfiguration {

    private Set<Integer> R_max;
    private Set<Integer> R_min;

    /**
     * Creates a weight configuration from the current view
     */
    WeightConfiguration(boolean isBFT, ServerViewController controller) {

        int n = controller.getCurrentViewN();

        R_max = new TreeSet<Integer>();
        R_min = new TreeSet<Integer>();

        View view = controller.getCurrentView();

        Map<Integer, Double> weights = view.getWeights();

        for (int i = 0; i < n; i++) {
            if (weights.get(i) == 1.00) {
                R_min.add(i);
            } else {
                R_max.add(i);
            }
        }
    }

    /**
     * Creates new Weight Config from replica array (permutation) assuming the V_max Replicas are listed first
     *
     * @param u nmuber of Vmax replicass: 2f (BFT) or f (CFT)
     * @param replicaSet all replicas
     */
    WeightConfiguration(int u, int[] replicaSet) {

        R_max = new TreeSet<Integer>();
        R_min = new TreeSet<Integer>();

        for (int i = 0; i < u; i++) {
            R_max.add(replicaSet[i]);
        }
        for (int i = u; i < replicaSet.length; i++) {
            R_min.add(replicaSet[i]);
        }

    }

    public static WeightConfiguration allPossibleWeightConfigurations(int u, int[] replicaSet){
        // TODO
        return null;
    }

    public Set<Integer> getR_max() {
        return R_max;
    }

    public Set<Integer> getR_min() {
        return R_min;
    }

    @Override
    public String toString() {
        return "WeightConfiguration{" +
                "R_max=" + R_max +
                ", R_min=" + R_min +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WeightConfiguration that = (WeightConfiguration) o;
        return Objects.equals(R_max, that.R_max) &&
                Objects.equals(R_min, that.R_min);
    }

    @Override
    public int hashCode() {
        return Objects.hash(R_max, R_min);
    }

}
