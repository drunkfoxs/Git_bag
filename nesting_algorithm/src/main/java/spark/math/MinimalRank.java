package spark.math;

/*
 * Given a set of input variables (double[] scores), 
 * MinimalRank help to rank the variable with the 
 * minimal value. 
 */

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.math.MathException;
import org.apache.commons.math.distribution.NormalDistribution;
import org.apache.commons.math.distribution.NormalDistributionImpl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MinimalRank {
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    List<Double> scores = new ArrayList<Double>();
    private double minimal = Double.MAX_VALUE;

    public void set(double score) {
        scores.add(score);
    }

    private void caculate() {
        if (scores.size() == 0)
            return;

        for (Double o : scores) {
            if (o < minimal) {
                minimal = o;
            }
        }
        logger.debug("minimal: " + minimal);
    }

    /**
     * @return
     */
    public double highestProbability() {
        caculate();
        return minimal;
    }
}