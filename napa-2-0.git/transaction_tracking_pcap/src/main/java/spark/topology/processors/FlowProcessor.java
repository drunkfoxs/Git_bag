package spark.topology.processors;

import java.util.HashMap;

import org.apache.wink.json4j.JSONArray;
import org.apache.wink.json4j.JSONObject;
import org.apache.wink.json4j.JSON;
import org.apache.wink.json4j.JSONException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import spark.pcap.packet.Packet;
import spark.topology.objects.*;

public class FlowProcessor implements java.io.Serializable {
	/*
	 * The example flow
	 * { 
	 * 		"tsEcr": "TCP Timestamp echo reply", 
	 * 		"protocol":"TCP, UDP HTTP/1.1 etc.", 
	 * 		"srcPort": "TCP source port", 
	 * 		"httpMethod":"PUT, GET, POST, DELETE", 
	 * 		"httpUri": "Http URI", 
	 * 		"dstPort": "TCP destination port, 
	 * 		"dst": "Destination IP", 
	 * 		"src": "Source IP",
	 * 		"httpType": "HTTP request or Http response", 
	 * 		"tsVal": "TCP Timestamp value"
	 * }
	 */
	
	private Logger logger = LoggerFactory.getLogger(getClass());

	public SplitFlow parseFlow(String jsonString) {
		String src = "";
		Integer srcPort = -1;
		String dst = "";
		Integer dstPort = -1;
		String protocol = "";
		Long tsVal = -1l;
		Long tsEcr = -1l;
		Long startTime = -1l;
		Long endTime = -1l;
		String httpUri = "";
		String httpMethod = "";
		String httpType = "";
		boolean ack = false;
		boolean request = false;
		boolean response = false;

		SplitFlow flow = new SplitFlow();
		SplitFlowAttributes attributes = flow.getSplitFlowAttributes();

		try {
			JSONObject rootObject = (JSONObject) JSON.parse(jsonString);
			tsEcr = Long.parseLong(rootObject.get("tsEcr").toString());
			protocol = rootObject.get("protocol").toString();
			srcPort = (Integer) rootObject.get("srcPort");
			httpMethod = rootObject.get("httpMethod").toString();
			httpUri = rootObject.getString("httpUri").toString();
			dstPort = (Integer) rootObject.get("dstPort");
			dst = rootObject.get("dst").toString();
			src = rootObject.get("src").toString();
			httpType = rootObject.get("httpType").toString();
			tsVal =  Long.parseLong(rootObject.get("tsVal").toString());
			ack = (boolean) rootObject.get("ack");
		} catch (NullPointerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error("The json String to be parsed: " + jsonString);
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error("The json String to be parsed: " + jsonString);
		}
		
		/*
		 * Generate the attributes
		 */

		attributes.setDst(dst);
		attributes.setDstPort(dstPort);
		attributes.setHttpMethod(httpMethod);
		attributes.setHttpType(httpType);
		attributes.setHttpUri(httpUri);
		attributes.setProtocol(protocol);
		attributes.setSrc(src);
		attributes.setSrcPort(srcPort);
		attributes.setTsEcr(tsEcr);
		attributes.setTsVal(tsVal);
		attributes.setIsAck(ack);
		
		/*
		 * Start Time for this set of request and response
		 */
		
		if (httpType.startsWith("Request")) {
			attributes.setStartTime(tsVal);		
			request = true;
		} else if (httpType.startsWith("Response")) {
			response = true;
		}
		
		/*
		 * Generate two nodes: src node & dst node
		 */
		
		if (src != ""){
			SplitFlowNode splitFlowSrcNode = new SplitFlowNode();
			splitFlowSrcNode.setId(src);
			splitFlowSrcNode.setIp(src);
			String srcIndex = new StringBuilder().append(src).toString();
			flow.updateSplitFlowNode(srcIndex, splitFlowSrcNode);
		}
		
		if (dst != ""){
			SplitFlowNode splitFlowDstNode = new SplitFlowNode();
			splitFlowDstNode.setId(dst);
			splitFlowDstNode.setIp(dst);
			String dstIndex = new StringBuilder().append(dst).toString();
			flow.updateSplitFlowNode(dstIndex, splitFlowDstNode);
		}
		
		/*
		 * Generate one edge
		 */
		
		if (src != "" && dst != ""){
			SplitFlowEdge edge = new SplitFlowEdge();
			String index = new StringBuilder().append(src).append("_")
					.append(srcPort).append("_").append(dst).append("_")
					.append(dstPort).append("_").append(httpType).append("_")
					.append(tsVal).toString();
			edge.setOutV(src);
			edge.setInV(dst);
			edge.setHttpMethod(httpMethod);
			edge.setHttpUri(httpUri);
			edge.setOutPort(srcPort);
			edge.setInPort(dstPort);
			edge.setOutTime(tsVal); 
			edge.setId(tsVal.toString());
			edge.setLabel(httpType);
			edge.setIsRequest(request);
			edge.setIsResponse(response);
			edge.setProtocol(protocol);
			flow.updateSplitFlowEdge(index, edge);
		}		
		
		return flow;
	}

}