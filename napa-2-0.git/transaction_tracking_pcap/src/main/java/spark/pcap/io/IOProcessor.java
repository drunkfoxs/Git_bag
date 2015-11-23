/**
 *
 * @author Haishan Wu
 * 
 */

package spark.pcap.io;

import javax.ws.rs.core.MediaType;

import org.apache.wink.client.ClientResponse;
import org.apache.wink.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IOProcessor implements java.io.Serializable {
	
	private String host;
	
	private String tenant;
	
	public IOProcessor(String host, String tenant) {
		this.host = host;
		this.tenant = tenant;
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private Logger logger = LoggerFactory.getLogger(getClass());
	
	private final static String DEFAULT_HOST = "napa-dev.sl.cloud9.ibm.com";
	private final static String DEFAULT_TENANT = "cloudsight";
	
	
	public final static String CHANNEL_METRIC = "metric";
	public final static String CHANNEL_TOPOLOGY = "topology";
	

	public void postToBroker(String content, String channel) {
		RestClient restClient = new RestClient();
		if (host == null) {
			host = DEFAULT_HOST;
		}
		if (tenant == null) {
			tenant = DEFAULT_TENANT;
		}
		String postDataURL = "http://" + host
				+ ":8081/broker/v0/data?tenant=" + tenant + "&type=" + channel;
		try {
			logger.info("POST to " + postDataURL);
			ClientResponse response = restClient.resource(postDataURL)
					.contentType(MediaType.APPLICATION_JSON)
					.accept(MediaType.APPLICATION_JSON).post(content);
			logger.info("Response: {}", response.getEntity(String.class));
			logger.info("Response status: {}", response.getStatusCode());

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/*
	 * Post to the default host POST
	 * http://napa-dev.sl.cloud9.ibm.com:8081/broker
	 * /v0/data?tenant=tivoli&type=topology
	 */
	public void postToBrokerTopology(String content) {
		postToBroker(content, CHANNEL_TOPOLOGY);
		
	}
	
	/*
	 * Post to the default host POST
	 * http://napa-dev.sl.cloud9.ibm.com:8081/broker
	 * /v0/data?tenant=tivoli&type=metric
	 */
	public void postToBrokerMetric(String content) {
		postToBroker(content, CHANNEL_METRIC);

	}


}
