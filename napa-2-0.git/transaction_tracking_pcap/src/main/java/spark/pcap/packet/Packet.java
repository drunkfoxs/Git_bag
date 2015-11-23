package spark.pcap.packet;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Packet extends HashMap<String, Object> {
	

	private static final long serialVersionUID = 8723206921174160146L;
	private static final String HTTP_ID = "HTTP";
	private static final String CASSANDRA_ID = "CASSANDRA";
	private static final String MONGOWIRE_ID = "MONGOWIRE";

	public static final String TIMESTAMP = "ts";
	public static final String TIMESTAMP_MICROS = "tsmicros";
    public static final String TS_USEC = "ts_usec";
    public static final String PACKET_DATA = "packet_data";
    public static final String IP_IDENTIFICATION = "ip_identification";
	public static final String TTL = "ttl";
	public static final String IP_VERSION = "ip_version";	
	public static final String IP_HEADER_LENGTH = "ip_header_length";	
	public static final String PROTOCOL = "protocol";
	public static final String TCP_HEADER_LENGTH = "tcp_header_length";
	public static final String TCP_SEQ = "tcp_seq";
	public static final String TCP_ACK = "tcp_ack";
	public static final String LEN = "len";
	public static final String UDPSUM = "udpsum";
	public static final String UDP_LENGTH = "udp_length";
	public static final String TCP_FLAG_NS = "tcp_flag_ns";
	public static final String TCP_FLAG_CWR = "tcp_flag_cwr";
	public static final String TCP_FLAG_ECE = "tcp_flag_ece";
	public static final String TCP_FLAG_URG = "tcp_flag_urg";
	public static final String TCP_FLAG_ACK = "tcp_flag_ack";
	public static final String TCP_FLAG_PSH = "tcp_flag_psh";
	public static final String TCP_FLAG_RST = "tcp_flag_rst";
	public static final String TCP_FLAG_SYN = "tcp_flag_syn";
	public static final String TCP_FLAG_FIN = "tcp_flag_fin";
	public static final String REASSEMBLED_FRAGMENTS = "reassembled_fragments";
	
	/*
	 * Header for NAPA packet parsing
	 */
	public static final String SRC_MAC = "src_mac";  //to be included
	public static final String DST_MAC = "dst_mac";  //to be included
	public static final String SRC = "src";
	public static final String DST = "dst";
	public static final String SRC_PORT = "src_port";
	public static final String DST_PORT = "dst_port";
	public static final String HTTP_DATA = "http_data"; //to be included
	public static final String TCP_OPTIONS_TSVAL = "tcp_options_tsval";
	public static final String TCP_OPTIONS_TSECR = "tcp_options_tsecr";
	public static final String DATA = "data";
	public static final String HTTP_URI = "http_uri";
	public static final String HTTP_METHOD= "http_method";
	public static final String HTTP_TYPE= "http_type";
	
	private       Logger logger	= LoggerFactory.getLogger(getClass());

	public Flow getFlow() {
		String src = (String)get(Packet.SRC);
		Integer srcPort = (Integer)get(Packet.SRC_PORT);
		String dst = (String)get(Packet.DST);
		Integer dstPort = (Integer)get(Packet.DST_PORT);
		String protocol = (String)get(Packet.PROTOCOL);
		String httpUri = (String)get(Packet.HTTP_URI);
		String httpMethod = (String)get(Packet.HTTP_METHOD);
		String httpType = (String)get(Packet.HTTP_TYPE);
		long tsval = -1;
		long tsecr = -1;
		Boolean ack = (Boolean)get(Packet.TCP_FLAG_ACK);
		
		if (get(Packet.TCP_OPTIONS_TSVAL) != null ){
			tsval = Long.parseLong(get(Packet.TCP_OPTIONS_TSVAL).toString());
		} 	
		if (get(Packet.TCP_OPTIONS_TSECR) != null){
			tsecr = Long.parseLong(get(Packet.TCP_OPTIONS_TSECR).toString());
		}
		if (protocol != null){
			if (protocol.startsWith(HTTP_ID) || protocol.startsWith(CASSANDRA_ID) || protocol.startsWith(MONGOWIRE_ID)) {
				ack = false; // This is not just an ACK
			}	
		} else {
			protocol = "unknown";
		}		
		return new Flow(src, srcPort, dst, dstPort, protocol, tsval, tsecr, httpUri, httpMethod, httpType, ack);
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		for (Map.Entry<String, Object> entry : entrySet()) {
			sb.append(entry.getKey());
			sb.append('=');
			sb.append(entry.getValue());
			sb.append(',');
		}
		if (sb.length() > 0)
			return sb.substring(0, sb.length() - 1);
		return "";
	}

	
}