import static org.junit.Assert.*;

import org.junit.Test;

import at.illecker.sentistorm.SentiStormTopology;


public class test {

	@Test
	public void testtopology() throws Exception {
	    String consumerKey = "IEhTcaONSfZyx0lIiXLLNHEXU";
	    String consumerSecret = "tWHPxM3fwBnDcAEH5JwnO5no9hFgofenG2B8WwEVuJkux4XA8f";
	    String accessToken = "2872327043-gCQDaQk2423UTt7FQxclG7qZZBcl8iMrobHlJWK";
	    String accessTokenSecret = "401nrunsVgTqgsJQkA4wsh1MPfAWGU2jd1eSvA8nHE5iD";
	    String[] args ={consumerKey,consumerSecret,accessToken,accessTokenSecret};		
	    SentiStormTopology.main(args);
	}
}
