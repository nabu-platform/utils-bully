package be.nabu.utils.bully;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;

import be.nabu.libs.http.HTTPException;

@Path(value = "/bully")
public class BullyREST {
	
	@Context
	private String localHost;
	
	@Context
	private BullyClient client;
	
	@Context
	private MasterController controller;
	
	@Context
	private Logger logger;
	
	@GET
	@Path(value = "/history")
	public BullyQueryOverview overview() {
		return client.getHistory();
	}
	
	@POST
	@Path(value = "/alive")
	public BullyQueryOverview alive(BullyQuery query) {
		logger.info("Server '" + query.getHost() + "' is checking in");
		client.push(query);
		return client.getHistory();
	}
	
	@POST
	@Path(value = "/inquiry")
	@Consumes(value = { MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
	// an incoming query
	public void inquiry(BullyQuery query) {
		logger.info("Inquiry from '" + query.getHost() + "'");
		if (query.getHost() == null) {
			throw new HTTPException(400, "Missing host");
		}
		else if (query.getHost().equals(localHost)) {
			throw new HTTPException(500, "Getting messages from same host");
		}
		else if (!client.hosts.contains(query.getHost())) {
			throw new HTTPException(405, "We are not part of the same cluster");
		}
		else {
			int comparison = query.getHost().compareTo(localHost);
			// the host in the query is higher ranked than this one
			// let's give him a sec to also send a victory, if not, we restart elections
			if (comparison > 0) {
				client.scheduleElection(false);
			}
			// the host is ranked lower, start an election immediately
			else {
				client.scheduleElection(true);
			}
		}
	}
	
	@POST
	@Path(value = "/victory")
	@Consumes(value = { MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
	public void victory(BullyQuery query) {
		logger.info("Victory from '" + query.getHost() + "'");
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
				controller.setMaster(query.getHost());
			}
			else {
				// a lower level server thinks it can become master, let's put an end to that immediately
				client.scheduleElection(true);
				throw new HTTPException(409, "Lower host is proclaiming victory");
			}
		}
	}
}
