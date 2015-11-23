package spark.pcap.processor;

import org.apache.spark.streaming.api.java.JavaStreamingContext;

public final class PcapCloser implements Runnable
{

	private JavaStreamingContext	jssc;

	public PcapCloser(JavaStreamingContext jssc)
	{
		this.jssc = jssc;
	}

	public void run()
	{
		this.jssc.stop();
		System.out.println("spark streaming service closed...");
	}

}
