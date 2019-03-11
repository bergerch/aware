package bftsmart.dynwheat.tests;

import bftsmart.dynwheat.decisions.WeightConfiguration;

import java.util.List;


public class WeightConfigurationTest {

    /**
     * Tests if generating all possible weight configurations works
     *
     * @author cb
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception {

        int[] replicaSet = {0, 1, 2, 3, 4};
        int u = 2;

        WeightConfiguration weightConfiguration = new WeightConfiguration(u, replicaSet);
        List<WeightConfiguration> weightConfigurations = weightConfiguration.allPossibleWeightConfigurations();

        for (WeightConfiguration w: weightConfigurations) {
            System.out.println(w);
        }

    }

}
