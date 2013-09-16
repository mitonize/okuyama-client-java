package jp.co.unisys.web.datastore.okuyama;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/example")
public class ExampleResource {
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response index() {
		return Response.ok(new Example("hello")).build();
	}

}
