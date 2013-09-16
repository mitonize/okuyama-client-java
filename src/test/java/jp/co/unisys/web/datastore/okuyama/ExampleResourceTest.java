package jp.co.unisys.web.datastore.okuyama;

import static org.junit.Assert.*;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;

import org.jboss.resteasy.test.BaseResourceTest;
import org.jboss.resteasy.test.TestPortProvider;
import org.junit.BeforeClass;
import org.junit.Test;

public class ExampleResourceTest extends BaseResourceTest {
	
	@BeforeClass
	public static void beforeClass() throws Exception {
		addPerRequestResource(ExampleResource.class);
	}

	@Test
	public void index() {
		Client c = ClientBuilder.newClient();
		String u = TestPortProvider.generateURL("/example");
		WebTarget target = c.target(u);
		
		Response r = target.request().get();
		assertEquals(200,  r.getStatus());
		Example e = r.readEntity(Example.class);
		assertEquals("hello", e.getName());
		
		r.close();
		c.close();
	}

}
