package spark.pcap.processor;

import java.io.Serializable;
import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;
import spark.pcap.io.PcapCustomReceiver;

public class PcapProcessor2 implements Serializable
{

	private static final long		serialVersionUID	= 1L;

	private static final Logger		LOGGER				= LoggerFactory
																.getLogger(PcapProcessor2.class);

	// private static final String DEFAULT_MASTER = "local[2]";

	// Currently start from the pcap file captured by Shriram
	// private static final String DATA_DIR =
	// "data/microservices-packet-captures/packet-traces.pcap";

	private static final Pattern	LINE				= Pattern
																.compile("\n");
	private static final Duration	WINDOW				= new Duration(30000);

	public void packetProcessing(int type, String source)
	{
		SparkConf conf = new SparkConf().setAppName("PacketProcessing");

		JavaStreamingContext jssc = new JavaStreamingContext(conf, WINDOW);

		// Parse packet stream
		LOGGER.info("Start reading the pcap packets ...");

		JavaReceiverInputDStream<String> lines = jssc
				.receiverStream(new PcapCustomReceiver(type, source));

		@SuppressWarnings("serial")
		JavaDStream<String> flows = lines
				.flatMap(new FlatMapFunction<String, String>()
				{
					public Iterable<String> call(String line) throws Exception
					{
						LOGGER.debug("[PcapProcessor] " + line);
						return Arrays.asList(LINE.split(line));
					}
				});

		// count each word's frequency as 1
		@SuppressWarnings("serial")
		JavaPairDStream<String, Integer> flowsWithID = flows
				.mapToPair(new PairFunction<String, String, Integer>()
				{
					public Tuple2<String, Integer> call(String flow)
							throws Exception
					{
						return new Tuple2<String, Integer>(flow, 1);
					}
				});

		@SuppressWarnings("serial")
		JavaPairDStream<String, Integer> flowCounts = flowsWithID
				.reduceByKey(new Function2<Integer, Integer, Integer>()
				{
					public Integer call(Integer f1, Integer f2)
							throws Exception
					{
						return f1 + f2;
					}
				});

		// flowCounts.foreach(new Function<JavaPairRDD<String, Integer>, Void>()
		// {
		// public Void call(JavaPairRDD<String, Integer> pair)
		// {
		// LOGGER.info("(" + pair.name() + "), (" + pair.count() + ")");
		// return null;
		// }
		// });

		flowCounts.print();

		// Stop streaming service when ctrl+c
		Thread closer = new Thread(new PcapCloser(jssc));
		Runtime.getRuntime().addShutdownHook(closer);

		jssc.start();
		jssc.awaitTermination();
	}

}
