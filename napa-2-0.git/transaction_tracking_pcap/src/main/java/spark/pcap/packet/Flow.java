package spark.pcap.packet;

import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;
import org.apache.wink.json4j.JSONArray;
import org.apache.wink.json4j.JSONObject;
import org.apache.wink.json4j.JSON;
import org.apache.wink.json4j.JSONException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Flow implements Comparable<Flow> {
	private String src = "";
	private Integer srcPort = -1;
	private String dst = "";
	private Integer dstPort = -1;
	private String protocol = "";
	private Long tsVal = -1l;
	private Long tsEcr = -1l;
	private String httpUri = "";
	private String httpMethod = "";
	private String httpType = "";
	private Boolean ack = false;
	private Logger logger = LoggerFactory.getLogger(getClass());

	public Flow(String src, Integer srcPort, String dst, Integer dstPort,
			String protocol) {
		this.src = src;
		this.srcPort = srcPort;
		this.dst = dst;
		this.dstPort = dstPort;
		this.protocol = protocol;
	}

	public Flow(String src, Integer srcPort, String dst, Integer dstPort,
			String protocol, Long tsVal, Long tsEcr, String httpUri,
			String httpMethod, String httpType, Boolean ack) {
		if (src != null) {
			this.src = src;
		}
		if (srcPort != null) {
			this.srcPort = srcPort;
		}
		if (dst != null) {
			this.dst = dst;
		}
		if (dstPort != null) {
			this.dstPort = dstPort;
		}
		if (tsVal != null) {
			this.tsVal = tsVal;
		}
		if (tsEcr != null) {
			this.tsEcr = tsEcr;
		}
		if (protocol != null) {
			this.protocol = protocol;
		}
		if (httpUri != null) {
			this.httpUri = httpUri;
		}
		if (httpType != null) {
			this.httpType = httpType;
		}
		if (httpMethod != null) {
			this.httpMethod = httpMethod;
		}
		if (ack != null) {
			this.ack = ack;
		}	

	}

	@Override
	public int compareTo(Flow o) {

		int r1 = ComparisonChain.start().compare(src, o.src)
				.compare(srcPort, o.srcPort).compare(dst, o.dst)
				.compare(dstPort, o.dstPort).compare(protocol, o.protocol)
				.result();
		if (r1 != 0) // try the other way
			return ComparisonChain.start().compare(src, o.dst)
					.compare(srcPort, o.dstPort).compare(dst, o.src)
					.compare(dstPort, o.srcPort).compare(protocol, o.protocol)
					.result();
		return 0;
	}

	public String flowKey() {
		return "<" + src + "," + srcPort + "," + dst + "," + dstPort + ","
				+ protocol + ">";
	}

	public String flowKeywithTS() {
		return "<" + src + "," + srcPort + "," + dst + "," + dstPort + ","
				+ protocol + "," + tsVal + "," + tsEcr + "," + httpUri + ","
				+ httpType + "," + httpMethod + ">";
	}

	/*
	 * { "tsEcr": "TCP Timestamp echo reply", "protocol":
	 * "TCP, UDP HTTP/1.1 etc.", "srcPort": "TCP source port", "httpMethod":
	 * "PUT, GET, POST, DELETE", "httpUri": "Http URI", "dstPort": "TCP
	 * destination port, "dst": "Destination IP", "src": "Source IP",
	 * "httpType": "HTTP request or Http response", "tsVal":
	 * "TCP Timestamp value" "ack":
	 * "true / false. Whether this is an ACK or not"}
	 */

	public JSONObject flowKeyinJson(){
		JSONObject flowJson = new JSONObject();
		try {
			flowJson.put("src", src);
			flowJson.put("srcPort", srcPort);
			flowJson.put("dst", dst);
			flowJson.put("dstPort", dstPort);
			flowJson.put("protocol", protocol);
			flowJson.put("tsVal", tsVal);
			flowJson.put("tsEcr", tsEcr);
			flowJson.put("httpUri", httpUri);
			flowJson.put("httpType", httpType);
			flowJson.put("httpMethod", httpMethod);
			flowJson.put("ack", ack);
		} catch (JSONException e) {
			logger.error(e.toString());
			e.printStackTrace();
		}
    	return flowJson;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this.getClass()).add("src", src)
				.add("srcPort", srcPort).add("dst", dst)
				.add("dstPort", dstPort).add("protocol", protocol)
				.add("tsVal", tsVal).add("tsEcr", tsEcr).toString();
	}
}