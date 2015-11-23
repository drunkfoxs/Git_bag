package spark.pcap.run;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import spark.pcap.processor.PcapProcessor2;

public class PacketPathDiscover2
{

	private static final Logger	LOGGER	= LoggerFactory
												.getLogger(PacketPathDiscover2.class);

	public static void main(String[] args) throws Exception
	{
		if (args.length < 2)
		{
			LOGGER.error("usage: PacketPathDiscover <captureType(1-file, 2-live)> <pcapfile|interface>");
			System.exit(-1);
		}

		int type = Integer.parseInt(args[0]);
		String source = args[1];

		LOGGER.info("Pcap source type:" + type + ", source: " + source);

		PcapProcessor2 pcap = new PcapProcessor2();
		pcap.packetProcessing(type, source);
	}

}
