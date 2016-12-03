package be.nabu.utils.bully;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.security.Principal;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import be.nabu.libs.http.api.HTTPResponse;
import be.nabu.libs.http.api.client.HTTPClient;
import be.nabu.libs.http.core.DefaultHTTPRequest;
import be.nabu.libs.types.api.ComplexType;
import be.nabu.libs.types.binding.xml.XMLBinding;
import be.nabu.libs.types.java.BeanInstance;
import be.nabu.libs.types.java.BeanResolver;
import be.nabu.utils.io.IOUtils;
import be.nabu.utils.mime.impl.FormatException;
import be.nabu.utils.mime.impl.MimeHeader;
import be.nabu.utils.mime.impl.PlainMimeContentPart;

public class BullyClient {

	private HTTPClient client;
	private List<String> hosts;
	private Principal principal;
	private boolean secure;
	private String host;
	private Logger logger = LoggerFactory.getLogger(getClass());
	private String rootPath;

	public BullyClient(String host, String rootPath, HTTPClient client, Principal principal, boolean secure, String...hosts) {
		this.host = host;
		this.rootPath = rootPath == null || rootPath.trim().isEmpty() ? "/" : rootPath;
		if (!this.rootPath.endsWith("/")) {
			this.rootPath += "/";
		}
		if (!this.rootPath.startsWith("/")) {
			this.rootPath = "/" + this.rootPath;
		}
		this.client = client;
		this.principal = principal;
		this.secure = secure;
		this.hosts = new ArrayList<String>(Arrays.asList(hosts));
		Collections.sort(this.hosts);
		Collections.reverse(this.hosts);
	}
	
	// start an election
	public void elect() {
		boolean potentialMasterFound = false;
		for (String host : hosts) {
			// it is ranked higher than this server
			int comparison = host.compareTo(this.host);
			if (comparison > 0) {
				HTTPResponse response = request("/bully/inquiry", new BullyQuery(host));
				// no response from the server, move on to the next
				if (response == null) {
					logger.warn("Did not get a response from '" + host + "', it is presumed to be down");
				}
				else if (response.getCode() == 400) {
					throw new RuntimeException("Received a 400 from the server");
				}
				// we have a new master!
				// wait for his victory command to properly announce him though
				else if (response.getCode() >= 200 && response.getCode() < 300) {
					logger.info("Potential master found: " + host);
					potentialMasterFound = true;
				}
				else {
					throw new RuntimeException("Received error code " + response.getCode() + " from '" + host + "'");
				}
			}
			// if we haven't found a master, proclaim victory
			else if (comparison < 0 && !potentialMasterFound) {
				request("/bully/victory", new BullyQuery(host));
			}
		}
	}
	
	private HTTPResponse request(String path, BullyQuery query) {
		String fullPath = this.rootPath + (path.startsWith("/") ? path.substring(1) : path);
		XMLBinding binding = new XMLBinding((ComplexType) BeanResolver.getInstance().resolve(BullyQuery.class), Charset.forName("UTF-8"));
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		try {
			binding.marshal(output, new BeanInstance<BullyQuery>(query));
		}
		catch (IOException e) {
			throw new RuntimeException("This should not happen!", e);
		}
		byte [] content = output.toByteArray();
		try {
			return client.execute(new DefaultHTTPRequest("POST", fullPath, 
				new PlainMimeContentPart(null, IOUtils.wrap(content, true), 
					new MimeHeader("Content-Length", "" + content.length),
					new MimeHeader("Content-Type", "application/xml"),
					new MimeHeader("Host", query.getHost())
				)), 
				principal,
				secure, 
				false
			);
		}
		catch (IOException e) {
			// this means the remote server can not be reached or it timed out, we assume it's dead
			logger.warn("Could not reach '" + query.getHost() + "' for election", e);
			return null;
		}
		catch (FormatException e) {
			logger.error("Could not format message", e);
			throw new RuntimeException(e);
		}
		catch (ParseException e) {
			logger.error("Could not parse message", e);
			throw new RuntimeException(e);
		}
	}
}
