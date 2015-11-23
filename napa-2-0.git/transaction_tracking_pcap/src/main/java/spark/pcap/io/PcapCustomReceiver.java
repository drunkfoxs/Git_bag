package spark.pcap.io;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.util.List;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.pcap4j.core.BpfProgram.BpfCompileMode;
import org.pcap4j.core.NotOpenException;
import org.pcap4j.core.PacketListener;
import org.pcap4j.core.PcapHandle;
import org.pcap4j.core.PcapNativeException;
import org.pcap4j.core.PcapNetworkInterface;
import org.pcap4j.core.PcapNetworkInterface.PromiscuousMode;
import org.pcap4j.core.PcapStat;
import org.pcap4j.core.Pcaps;
import org.pcap4j.packet.IpV4Packet;
import org.pcap4j.packet.IpV4Packet.IpV4Header;
import org.pcap4j.packet.Packet;
import org.pcap4j.packet.TcpPacket;
import org.pcap4j.packet.UdpPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import spark.pcap.packet.Flow;
//import spark.pcap.packet.Packet;
import spark.pcap.readers.PcapReader;

class PrintStats extends Thread
{
	PcapHandle			handle;
	PcapCustomReceiver	counter;
	//private Logger		logger	= LoggerFactory.getLogger(getClass());

	PrintStats(PcapHandle h, PcapCustomReceiver c)
	{
		handle = h;
		counter = c;
	}

	@Override
	public void run()
	{
		PcapStat ps;
		if (handle == null)
		{
			//logger.info("Empty handle");
			return;
		}
		try
		{
			ps = handle.getStats();
			//logger.info("ps_recv: " + ps.getNumPacketsReceived());
			//logger.info("ps_drop: " + ps.getNumPacketsDropped());
			//logger.info("ps_ifdrop: " + ps.getNumPacketsDroppedByIf());
			//logger.info("num_packets: " + counter.getNumPackets());
			// handle.close();
		}
		catch (PcapNativeException e)
		{
			e.printStackTrace();
		}
		catch (NotOpenException e)
		{
			e.printStackTrace();
		}
	}
}

public class PcapCustomReceiver extends Receiver<String> implements
		PacketListener
{

	private static final long	serialVersionUID	= 1L;

	private static long			POLLPERIODINMILLIS	= 10;							// ms
	private Logger				logger				= LoggerFactory
															.getLogger(getClass());
	String						dataDir				= "";
	String						host				= "9.186.58.215";
	int							port				= 80;
	private static final String	COUNT_KEY			= PcapCustomReceiver.class
															.getName()
															+ ".count";
	private static final int	COUNT				= Integer.getInteger(
															COUNT_KEY, -1);

	private static final String	READ_TIMEOUT_KEY	= PcapCustomReceiver.class
															.getName()
															+ ".readTimeout";
	private static final int	READ_TIMEOUT		= Integer.getInteger(
															READ_TIMEOUT_KEY,
															10);					// [ms]

	private static final String	SNAPLEN_KEY			= PcapCustomReceiver.class
															.getName()
															+ ".snaplen";
	private static final int	SNAPLEN				= Integer.getInteger(
															SNAPLEN_KEY, 256);		// [bytes]

	public static final int		CAPTURE_TYPE_FILE	= 1;
	public static final int		CAPTURE_TYPE_LIVE	= 2;

	String						iface				= "";
	String						filter				= "ip";
	int							input_type;
	int							num_packets;

	public PcapCustomReceiver(int capture_type, String source)
	{
		super(StorageLevel.MEMORY_AND_DISK_SER());
		input_type = capture_type;
		num_packets = 0;
		if (source != "")
		{
			if (capture_type == CAPTURE_TYPE_FILE)
				dataDir = source;
			else if (capture_type == CAPTURE_TYPE_LIVE) iface = source;
		}
	}

	public void onStart() {

		//logger.debug("Prepare to read the pcap files from " + dataDir);

		// Start the thread that receives data over a connection
		new Thread() {
			@Override
			public void run() {
				if (input_type == CAPTURE_TYPE_FILE) {
				 while (true) {
					//receive(); //receive packet from socket
						readDataFromPcapFiles();
						//readDataFromPcapFileStreams();
						try {
							Thread.sleep(POLLPERIODINMILLIS);
						} catch (InterruptedException ex) {

						}
					} 
				} else if (input_type == CAPTURE_TYPE_LIVE) {
						try {
							readDataFromNIC();
						} catch (PcapNativeException | NotOpenException e) {
							e.printStackTrace();
						}
				}
			}
		}.start();

	}

	public void onStop()
	{
		// There is nothing much to do as the thread calling receive()
		// is designed to stop by itself isStopped() returns false
	}

	int getNumPackets()
	{
		return num_packets;
	}

	@Override
	public void gotPacket(Packet packet)
	{
		// Get the flow key and sluggify it
		IpV4Header iph = packet.get(IpV4Packet.class).getHeader();
		UdpPacket udp = packet.get(UdpPacket.class);
		TcpPacket tcp = packet.get(TcpPacket.class);
		Integer srcPort = null, dstPort = null;
		if (udp != null)
		{
			srcPort = (int) udp.getHeader().getSrcPort().valueAsInt();
			dstPort = (int) udp.getHeader().getDstPort().valueAsInt();
		}
		else if (tcp != null)
		{
			srcPort = (int) tcp.getHeader().getSrcPort().valueAsInt();
			dstPort = (int) tcp.getHeader().getDstPort().valueAsInt();
		}
		
		Flow f = new Flow(iph.getSrcAddr().toString(), srcPort, iph
				.getDstAddr().toString(), dstPort, iph.getProtocol()
				.toString());

		logger.debug("Transfer flow to RDD: " + f.flowKey());
		
		store(f.flowKey() + "\n");
		// store(f);
		num_packets++;
	}

	private void readDataFromNIC() throws PcapNativeException,
			NotOpenException
	{
		logger.info(COUNT_KEY + ": " + COUNT);
		logger.info(READ_TIMEOUT_KEY + ": " + READ_TIMEOUT);
		logger.info(SNAPLEN_KEY + ": " + SNAPLEN);
		logger.info("\n");
		
		List<PcapNetworkInterface> allDevs = null;
		allDevs = Pcaps.findAllDevs();

		if (allDevs == null || allDevs.size() == 0)
		{
			logger.error("No NIF to capture.");
			return;
		}
		else
		{
			logger.info("List all the NIF(s): " + allDevs.toString());
		}

		PcapNetworkInterface nif = null;

		for (PcapNetworkInterface i : allDevs)
		{
			if (i.getName().equals(iface))
			{
				nif = i;
				break;
			}
		}

		if (nif == null)
		{
			logger.error("Invalid network interface:" + iface);
			return;
		}

		logger.info("Start capturing packets from: " + nif.getName() + "("
				+ nif.getDescription() + ")@" + nif.getAddresses().toString());

		final PcapHandle handle = nif.openLive(SNAPLEN,
				PromiscuousMode.PROMISCUOUS, READ_TIMEOUT);

		if (filter.length() != 0)
		{
			handle.setFilter(filter, BpfCompileMode.OPTIMIZE);
		}

		PrintStats ps = new PrintStats(handle, this);
		Runtime.getRuntime().addShutdownHook(ps);

		try
		{
			handle.loop(COUNT, this);
		}
		catch (InterruptedException e)
		{
			// e.printStackTrace();
		}

		// PcapStat ps = handle.getStats();
		// logger.info("ps_recv: " + ps.getNumPacketsReceived());
		// logger.info("ps_drop: " + ps.getNumPacketsDropped());
		// logger.info("ps_ifdrop: " + ps.getNumPacketsDroppedByIf());
		// logger.info("num_packets: " + num_packets);

	}

	private void readDataFromPcapFileStreams()
	{
		logger.debug("readDataFromPcapFileStreams: Start reading the pcap files from "
				+ dataDir);
		InputStream is = null;
		try
		{
			try
			{
				is = new FileInputStream(dataDir);
				byte[] b = new byte[is.available()];
				is.read(b);
				logger.debug("---- pcap streams ----" + new String(b));
				store(new String(b));
			}
			catch (FileNotFoundException e)
			{
				e.printStackTrace();
			}
			catch (IOException e)
			{
				e.printStackTrace();
			}
		}
		finally
		{
			if (is != null) try
			{
				is.close();
			}
			catch (IOException e)
			{
				e.printStackTrace();
			}
		}
	}

	/*
	 * Read the Pcap file streams from the disk
	 */
	private void readDataFromPcapFiles()
	{
		/*logger.debug("readDataFromPcapFiles: Start reading the pcap files from "
				+ dataDir);*/

		InputStream is = null;
		String pcapReaderClass = "spark.pcap.readers.PcapReader";
		String packetHeader = null;

		try
		{
			long packets = 0;
			is = new FileInputStream(dataDir);
			PcapReader reader = initPcapReader(pcapReaderClass,
					new DataInputStream(is));
			for (spark.pcap.packet.Packet packet : reader)
			{
				store(packet.getFlow().flowKeyinJson().toString() + "\n");
				
				//logger.info(packet.getFlow().flowKeyinJson().toString() + "\n");
				packets++;
				//logger.info("************Packets = " + packets);
			}
		}
		catch (FileNotFoundException e)
		{
			logger.error("Can't read the file from " + dataDir);
			e.printStackTrace();
		}
		finally
		{
			if (is != null) try
			{
				is.close();
			}
			catch (IOException e)
			{
				e.printStackTrace();
			}
		}

	}

	private PcapReader initPcapReader(String className, DataInputStream is)
	{
		try
		{
			@SuppressWarnings("unchecked")
			Class<? extends PcapReader> pcapReaderClass = (Class<? extends PcapReader>) Class
					.forName(className);
			Constructor<? extends PcapReader> pcapReaderConstructor = pcapReaderClass
					.getConstructor(DataInputStream.class);
			return pcapReaderConstructor.newInstance(is);
		}
		catch (Exception e)
		{
			e.printStackTrace();
			return null;
		}
	}

	// /** Create a socket connection and receive data until receiver is stopped
	// */
	// private void receive() {
	// Socket socket = null;
	// String userInput = null;
	//
	// try {
	// // connect to the server
	// socket = new Socket(host, port);
	//
	// BufferedReader reader = new BufferedReader(new InputStreamReader(
	// socket.getInputStream()));
	//
	// // Until stopped or connection broken continue reading
	// while (!isStopped() && (userInput = reader.readLine()) != null) {
	// logger.info("Received data '" + userInput + "'");
	// store(userInput);
	// }
	// reader.close();
	// socket.close();
	//
	// // Restart in an attempt to connect again when server is active
	// // again
	// restart("Trying to connect again");
	// } catch (ConnectException ce) {
	// // restart if could not connect to server
	// restart("Could not connect", ce);
	// } catch (Throwable t) {
	// // restart if there is any other error
	// restart("Error receiving data", t);
	// }
	// }

}
