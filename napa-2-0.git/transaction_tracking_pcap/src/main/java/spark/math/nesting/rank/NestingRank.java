package spark.math.nesting.rank;

/*
 * Given the assumption that the input variables (double[] scores) 
 * are normal distribution, NestingRank help to rank the probability
 * of specific variable. 
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

public class NestingRank {
	private Logger logger = LoggerFactory.getLogger(this.getClass());
	double mean = 0;
	double diffSquaredMean = 0;
	List<Double> scoreDiffSquaredArray;
	List<Double> scores = new ArrayList<Double>();
	double sampleStandardDeviation = 0; 
	private NormalDistribution normalDistribution;

	public void set(double score) {
		scores.add(score);
	}
	
	private void caculate() {
		if(scores.size() == 0)
			return;
		scoreDiffSquaredArray = new ArrayList<Double>();
		
		for (Double o : scores) {
			mean += o;
		}
		mean = mean / scores.size();
		
		for(int i = 0; i < scores.size(); i++){
			scoreDiffSquaredArray.add(Math.pow(scores.get(i) - mean, 2));
			diffSquaredMean = diffSquaredMean + scoreDiffSquaredArray.get(i);
		} 
		
		sampleStandardDeviation = Math.sqrt(diffSquaredMean/ (scores.size() - 1));
		
		logger.debug("mean: " + mean);
		logger.debug("Sample Standard Deviation: " + sampleStandardDeviation); 
		
		if(sampleStandardDeviation == 0)
			sampleStandardDeviation = 0.1; // In this case, randomly pick up one parent will be enough	
		
		normalDistribution = new NormalDistributionImpl(mean, sampleStandardDeviation);	
		
	}

	/**
	 * 
	 * @return
	 */
	public double highestProbability() {
		Map<Double, Double> rankList = rank();
		double highestRankResult = 0;
		double result = 0;
		for(Entry<Double, Double> o : rankList.entrySet()) {
			if(o.getValue() > highestRankResult) {
				highestRankResult = o.getValue();
				result = o.getKey();
			}
		}
		
		return result;
	}
	
	public Map<Double, Double> rank() {
		
		caculate();
		
		Map<Double, Double> result = new HashMap<Double, Double>();
		for (Double o : scores) {
			if(normalDistribution != null)
				result.put(o, normalDistribution.density(o));
		}
		return result;
	}

}
