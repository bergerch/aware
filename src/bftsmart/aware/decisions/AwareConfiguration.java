package bftsmart.aware.decisions;

import bftsmart.aware.monitoring.Monitor;

import java.util.Objects;

/**
 * AWARE Configuration includes a weight configuration and a leader selection
 *
 * @author cb
 */
public class AwareConfiguration implements Comparable {

    // A AWARE config consists of a weight distribution and leader selection
    private WeightConfiguration weightConfiguration;
    private int leader;

    // predicted latency for this configuration
    private long predictedLatency = Monitor.MISSING_VALUE;

    public AwareConfiguration(WeightConfiguration weightConfiguration, int leader, long predictedLatency) {
        this.weightConfiguration = weightConfiguration;
        this.leader = leader;
        this.predictedLatency = predictedLatency;
    }

    public AwareConfiguration(WeightConfiguration weightConfiguration, int leader) {
        this.weightConfiguration = weightConfiguration;
        this.leader = leader;
    }

    @Override
    public int compareTo(Object o) {
        return Long.compare(this.predictedLatency, ((AwareConfiguration) o).predictedLatency);
    }

    public WeightConfiguration getWeightConfiguration() {
        return weightConfiguration;
    }

    public void setWeightConfiguration(WeightConfiguration weightConfiguration) {
        this.weightConfiguration = weightConfiguration;
    }

    public int getLeader() {
        return leader;
    }

    public void setLeader(int leader) {
        this.leader = leader;
    }

    public long getPredictedLatency() {
        return predictedLatency;
    }

    public void setPredictedLatency(long predictedLatency) {
        this.predictedLatency = predictedLatency;
    }

    @Override
    public String toString() {
        String result =  weightConfiguration.toString() + " with leader " + leader + " ";
        if (predictedLatency != Monitor.MISSING_VALUE)
            result += "and latency of " + predictedLatency;

        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AwareConfiguration that = (AwareConfiguration) o;
        return leader == that.leader &&
                weightConfiguration.equals(that.weightConfiguration);
    }

    @Override
    public int hashCode() {
        return Objects.hash(weightConfiguration, leader);
    }
}
