/*
* Copyright (C) 2016 Alexander Verbruggen
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Lesser General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU Lesser General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public License
* along with this program. If not, see <https://www.gnu.org/licenses/>.
*/

package be.nabu.utils.bully;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadMXBean;
import java.nio.charset.Charset;
import java.security.Principal;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import be.nabu.libs.events.api.EventHandler;
import be.nabu.libs.http.api.HTTPRequest;
import be.nabu.libs.http.api.HTTPResponse;
import be.nabu.libs.http.api.client.HTTPClient;
import be.nabu.libs.http.core.DefaultHTTPRequest;
import be.nabu.libs.http.server.rest.RESTHandler;
import be.nabu.libs.types.api.ComplexContent;
import be.nabu.libs.types.api.ComplexType;
import be.nabu.libs.types.binding.api.Window;
import be.nabu.libs.types.binding.xml.XMLBinding;
import be.nabu.libs.types.java.BeanInstance;
import be.nabu.libs.types.java.BeanResolver;
import be.nabu.utils.bully.utils.WaitAndElect;
import be.nabu.utils.io.IOUtils;
import be.nabu.utils.mime.api.ContentPart;
import be.nabu.utils.mime.impl.FormatException;
import be.nabu.utils.mime.impl.MimeHeader;
import be.nabu.utils.mime.impl.PlainMimeContentPart;

public class BullyClient {

	private HTTPClient client;
	List<String> hosts;
	private Principal principal;
	private boolean secure;
	private String host;
	private Logger logger = LoggerFactory.getLogger(getClass());
	private String rootPath;
	private MasterController controller;
	private Thread waitAndElectThread;
	private long victoryTimeout;
	private List<MasterFuture> futures = new ArrayList<MasterFuture>();
	private volatile String currentMaster;
	private WaitAndElect waitAndElect;
	private Thread heartBeat;
	private RuntimeMXBean runtimeMXBean;
	private OperatingSystemMXBean operatingSystemMXBean;
	private ThreadMXBean threadMXBean;
	private MemoryMXBean memoryMXBean;
	// keep 60 elements in the history
	private int historySize;
	private Map<String, List<BullyQuery>> queries = new HashMap<String, List<BullyQuery>>();
	private BullyQueryOverview overview;
	private long heartBeatInterval = 60*1000;

	public BullyClient(String host, String rootPath, final MasterController controller, Long victoryTimeout, HTTPClient client, Principal principal, boolean secure, String...hosts) {
		this(host, rootPath, controller, victoryTimeout, client, principal, secure, Arrays.asList(hosts));
	}
	
	public BullyClient(String host, String rootPath, final MasterController controller, Long victoryTimeout, HTTPClient client, Principal principal, boolean secure, List<String> hosts) {
		this.host = host;
		this.controller = new MasterController() {
			@Override
			public void setMaster(String master) {
				synchronized(BullyClient.this) {
					// reset the current master if we are going for an election
					// during this time there is no master, we are in limbo
					if (master == null) {
						logger.info("Unsetting master");
						currentMaster = null;
						if (controller != null) {
							controller.setMaster(null);
						}
						stopHeartbeat();
					}
					else {
						logger.info("Setting master to '" + master + "'");
						if (cancelElection()) {
							// set locally before we set in the controller, that way anyone listening can do isMaster() properly
							currentMaster = master;
							if (controller != null) {
								controller.setMaster(master);
							}
							// resolve the futures
							synchronized (futures) {
								for (MasterFuture future : futures) {
									future.master = master;
								}
								futures.clear();
							}
							// make sure we have a heartbeat to the master
							startHeartbeat();
						}
						else {
							logger.warn("Could not cancel election, not accepting '" + master + "' as new master");
						}
					}
				}
			}
		};
		this.victoryTimeout = victoryTimeout == null || victoryTimeout == 0 ? 60*1000 : victoryTimeout;
		// we want to keep roughly one hours worth of data
		this.historySize = (int) ((60*1000*60) / this.victoryTimeout);
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
		this.hosts = new ArrayList<String>(hosts);
		Collections.sort(this.hosts);
		Collections.reverse(this.hosts);
		
		runtimeMXBean = ManagementFactory.getRuntimeMXBean();
		operatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean();
		threadMXBean = ManagementFactory.getThreadMXBean();
		memoryMXBean = ManagementFactory.getMemoryMXBean();
	}
	
	public EventHandler<HTTPRequest, HTTPResponse> newHandler() {
		// strip the ending "/" if necessary
		return new RESTHandler(rootPath.equals("/") ? rootPath : rootPath.substring(0, rootPath.length() - 1), BullyREST.class, null, this, this.host, controller, logger);
	}
	
	public boolean isCurrentMaster() {
		return currentMaster != null && host.equals(currentMaster);
	}
	
	public String getCurrentMaster() {
		return currentMaster;
	}
	
	private class MasterFuture implements Future<String> {

		private volatile String master;
		
		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			return cancelElection();
		}

		@Override
		public boolean isCancelled() {
			return false;
		}

		@Override
		public boolean isDone() {
			return master != null;
		}

		@Override
		public String get() throws InterruptedException, ExecutionException {
			try {
				return get(365, TimeUnit.DAYS);
			}
			catch (TimeoutException e) {
				throw new RuntimeException("You have a lot of patience my friend", e);
			}
		}

		@Override
		public String get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
			if (master != null) {
				return master;
			}
			Date started = new Date();
			timeout = TimeUnit.MILLISECONDS.convert(timeout, unit);
			while (new Date().getTime() - started.getTime() < timeout) {
				if (master != null) {
					return master;
				}
				Thread.sleep(Math.min(timeout - (new Date().getTime() - started.getTime()), 25));
			}
			return master;
		}
	}
	
	public Future<String> getMaster() {
		MasterFuture future = new MasterFuture();
		synchronized(futures) {
			future.master = currentMaster;
			// await future resolving
			if (future.master == null) {
				futures.add(future);
			}
		}
		return future;
	}
	
	public String getHost() {
		return host;
	}
	
	public List<String> getHosts() {
		return hosts;
	}
	
	// start an election
	public Future<String> elect() {
		logger.info("Starting elections");
		
		// unset the wait thread, we may need to start a new one
		waitAndElectThread = null;
		
		// unset master while we elect a new one
		controller.setMaster(null);
		
		boolean potentialMasterFound = false;
		boolean amIMaster = true;
		for (String host : hosts) {
			int comparison = host.compareTo(this.host);
			// it is ranked higher than this server
			if (comparison > 0) {
				HTTPResponse response = request(host, "/bully/inquiry", newBullyQuery());
				// no response from the server, move on to the next
				if (response == null) {
					logger.warn("Did not get a response from '" + host + "', it is presumed to be down");
				}
				else if (response.getCode() == 400) {
					throw new RuntimeException("Received a 400 from the server");
				}
				else if (response.getCode() == 405) {
					logger.error("Host '" + host + "' claims he is not in this cluster");
				}
				// we have a new master!
				// wait for his victory command to properly announce him though
				else if (response.getCode() >= 200 && response.getCode() < 300) {
					logger.info("Potential master found: " + host);
					// we need to make sure the master proclaims himself within the timeout
					// otherwise new elections will be held
					scheduleElection(false);
					potentialMasterFound = true;
				}
				else {
					throw new RuntimeException("Received error code " + response.getCode() + " from '" + host + "'");
				}
			}
			// if we haven't found a master, proclaim victory
			else if (comparison < 0 && !potentialMasterFound) {
				HTTPResponse response = request(host, "/bully/victory", newBullyQuery());
				if (response == null) {
					logger.warn("Did not get a response from '" + host + "', it is presumed to be down");
				}
				else if (response.getCode() == 400) {
					throw new RuntimeException("Received a 400 from the server");
				}
				// we fucked up
				else if (response.getCode() == 409) {
					logger.error("Received a 409 from another participant indicating one of us has wrong data");
					amIMaster = false;
				}
			}
		}
		MasterFuture future = new MasterFuture();
		synchronized(futures) {
			futures.add(future);
		}
		if (amIMaster && !potentialMasterFound) {
			controller.setMaster(host);
		}
		else if (!amIMaster && !potentialMasterFound) {
			logger.error("Failed to become master but no other master has been found");
		}
		return future;
	}

	private BullyQuery newBullyQuery() {
		BullyQuery bullyQuery = new BullyQuery(this.host);
		if (runtimeMXBean != null) {
			bullyQuery.setUptime(runtimeMXBean.getUptime());
		}
		if (operatingSystemMXBean != null) {
			bullyQuery.setLoad(operatingSystemMXBean.getSystemLoadAverage() / operatingSystemMXBean.getAvailableProcessors());
		}
		if (threadMXBean != null) {
			bullyQuery.setCurrentThreadCount(threadMXBean.getThreadCount());
		}
		if (memoryMXBean != null) {
			MemoryUsage heapMemoryUsage = memoryMXBean.getHeapMemoryUsage();
			bullyQuery.setHeapUsed(heapMemoryUsage.getUsed());
			MemoryUsage nonHeapMemoryUsage = memoryMXBean.getNonHeapMemoryUsage();
			bullyQuery.setNonHeapUsed(nonHeapMemoryUsage.getUsed());
		}
		return bullyQuery;
	}
	
	void startHeartbeat() {
		if (heartBeat == null) {
			synchronized(this) {
				if (heartBeat == null) {
					heartBeat = new Thread(new Runnable() {
						@SuppressWarnings("unchecked")
						@Override
						public void run() {
							while (!Thread.interrupted()) {
								try {
									Thread.sleep(heartBeatInterval);
								}
								catch (InterruptedException e1) {
									break;
								}
								// only poll _other_ servers
								if (currentMaster != null && !currentMaster.equals(host)) {
									try {
										HTTPResponse response = request(currentMaster, "/bully/alive", newBullyQuery());
										if (response.getCode() >= 200 && response.getCode() < 300) {
											logger.debug("Heartbeat to '" + currentMaster + "' is ok");
											XMLBinding binding = new XMLBinding((ComplexType) BeanResolver.getInstance().resolve(BullyQueryOverview.class), Charset.defaultCharset());
											ComplexContent unmarshal = binding.unmarshal(IOUtils.toInputStream(((ContentPart) response.getContent()).getReadable()), new Window[0]);
											overview = ((BeanInstance<BullyQueryOverview>) unmarshal).getUnwrapped();
										}
									}
									// master is having issues
									catch (Exception e) {
										logger.warn("Master '" + currentMaster + "' failed to respond to heartbeat, starting new election");
										scheduleElection(true);
									}
								}
								// push a history entry for ourselves so everyone knows how we are doing
								else {
									push(newBullyQuery());
								}
							}
						}
					});
					heartBeat.start();
				}
			}
		}
	}
	
	public long getHeartBeatInterval() {
		return heartBeatInterval;
	}

	public void setHeartBeatInterval(long heartBeatInterval) {
		this.heartBeatInterval = heartBeatInterval;
	}

	void stopHeartbeat() {
		if (heartBeat != null) {
			synchronized(this) {
				if (heartBeat != null) {
					heartBeat.interrupt();
					heartBeat = null;
				}
			}
		}
	}
	
	public void scheduleElection(boolean immediate) {
		if (waitAndElectThread == null || !waitAndElectThread.isAlive()) {
			synchronized(this) {
				if (waitAndElectThread == null || !waitAndElectThread.isAlive()) {
					waitAndElect = new WaitAndElect(this, immediate ? 0 : victoryTimeout);
					waitAndElectThread = new Thread(waitAndElect);
					waitAndElectThread.start();
				}
			}
		}
	}
	
	boolean cancelElection() {
		if (waitAndElectThread != null && waitAndElectThread.isAlive() && !waitAndElect.isElecting()) {
			synchronized(this) {
				if (waitAndElectThread != null && waitAndElectThread.isAlive() && !waitAndElect.isElecting()) {
					waitAndElectThread.interrupt();
					waitAndElectThread = null;
					// we succeeded in canceling if the election has not started yet
					return !waitAndElect.isElecting();
				}
			}
		}
		return true;
	}
	
	private HTTPResponse request(String host, String path, BullyQuery query) {
		String fullPath = this.rootPath + (path.startsWith("/") ? path.substring(1) : path);
		// use the default charset, all servers should be configured the same
		XMLBinding binding = new XMLBinding((ComplexType) BeanResolver.getInstance().resolve(BullyQuery.class), Charset.defaultCharset());
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
					new MimeHeader("Host", host)
				)), 
				principal,
				secure, 
				false
			);
		}
		catch (IOException e) {
			// this means the remote server can not be reached or it timed out, we assume it's dead
			logger.debug("Could not reach '" + host + "' for election, assuming it is down", e);
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
	
	List<BullyQuery> getHistory(String host) {
		List<BullyQuery> list = queries.get(host);
		return list == null ? null : new ArrayList<BullyQuery>(list);
	}
	
	public BullyQueryOverview getHistory() {
		// if there is no master or we are not the master, simply send back the overview we got from the master
		if (currentMaster == null || !currentMaster.equals(host)) {
			return overview;
		}
		// otherwise build it
		BullyQueryOverview overview = new BullyQueryOverview();
		overview.setLists(new ArrayList<BullyQueryList>());
		for (String host : hosts) {
			List<BullyQuery> history = getHistory(host);
			if (history != null) {
				BullyQueryList list = new BullyQueryList();
				list.setHost(host);
				list.setQueries(history);
				overview.getLists().add(list);
			}
		}
		return overview;
	}
	
	void push(BullyQuery query) {
		if (!queries.containsKey(query.getHost())) {
			synchronized(queries) {
				if (!queries.containsKey(query.getHost())) {
					queries.put(query.getHost(), new ArrayList<BullyQuery>());
				}
			}
		}
		List<BullyQuery> list = queries.get(query.getHost());
		synchronized(list) {
			if (list.size() > historySize) {
				list.remove(0);
			}
			list.add(query);
		}
	}
}
