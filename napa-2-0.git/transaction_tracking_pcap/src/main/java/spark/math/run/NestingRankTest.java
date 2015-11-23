package spark.math.run;

import org.apache.commons.math.MathException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import spark.math.nesting.rank.NestingRank;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;


public class NestingRankTest {
	private static Logger logger = LoggerFactory.getLogger(NestingRankTest.class);
	static int numOfScores;
	
	static double nestingProbability;
	
	public static void main(String[] args) throws MathException
	{
		NestingRank nestingRank = new NestingRank();
		
		System.out.print("Number of Scores (int): ");
		Scanner numOfScoresIN = new Scanner(System.in);
		
		numOfScores = numOfScoresIN.nextInt();
		
		System.out.print("Enter scores with spaces in between: ");
		Scanner scoresIN = new Scanner(System.in);
		
		for(int i = 0; i < numOfScores; i++){
			if(scoresIN.hasNextDouble()){
				double v = scoresIN.nextDouble();
				nestingRank.set(v);
				logger.info("The scores that have been received: " + v);
			}
		}
		
		scoresIN.close();
		numOfScoresIN.close();
		
		Map<Double, Double> rankList = nestingRank.rank();
		for(Entry<Double, Double> o : rankList.entrySet()){
			
			logger.info("The nesting probability for " + o.getKey() + " is " + o.getValue());
		}
		
		logger.info("Score has the highest nesting probability: " + nestingRank.highestProbability());
	}	

}