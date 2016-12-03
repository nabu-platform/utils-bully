package be.nabu.utils.bully;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import be.nabu.libs.http.HTTPException;
import be.nabu.utils.bully.utils.WaitAndElect;

@Path(value = "/bully")
public class BullyREST {
	
	@Context
	private String localHost;
	
	@Context
	private BullyClient client;
	
	@Context
	private MasterController controller;
	
	@Context
	private Long timeout;
	
	private Thread waitAndElectThread;
	
	@POST
	@Path(value = "/inquiry")
	@Consumes(value = { MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
	// an incoming query
	public void query(BullyQuery query) {
		if (query.getHost() == null) {
			throw new HTTPException(400, "Missing host");
		}
		else if (query.getHost().equals(localHost)) {
			throw new HTTPException(500, "Getting messages from same host");
		}
		else {
			int comparison = query.getHost().compareTo(localHost);
			// the host in the query is higher ranked than this one
			if (comparison > 0) {
				waitAndElectThread = new Thread(new WaitAndElect(client, timeout == null ? 60*1000 : timeout));
				waitAndElectThread.start();
			}
			// the host is ranked lower, start an election immediately
			else {
				waitAndElectThread = new Thread(new WaitAndElect(client, 0));
				waitAndElectThread.start();
			}
		}
	}
	
	@POST
	@Path(value = "/victory")
	@Consumes(value = { MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
	public void victory(BullyQuery query) {
		if (query.getHost() == null) {
			throw new HTTPException(400, "Missing host");
		}
		else if (query.getHost().equals(localHost)) {
			throw new HTTPException(500, "Getting messages from same host");
		}
		else {
			int comparison = query.getHost().compareTo(localHost);
			// the host in the query is higher ranked than this one, it's ok
			if (comparison > 0) {
				// interrupt any election still waiting
				if (waitAndElectThread != null && waitAndElectThread.isAlive()) {
					waitAndElectThread.interrupt();
				}
				controller.setMaster(query.getHost());
			}
			else {
				waitAndElectThread = new Thread(new WaitAndElect(client, 0));
				waitAndElectThread.start();
				throw new HTTPException(409, "Lower host is proclaiming victory");
			}
		}
	}
}
