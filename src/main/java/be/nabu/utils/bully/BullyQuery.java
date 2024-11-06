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

import java.util.Date;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "query")
public class BullyQuery {
	private String host;
	private Date created;
	private double load, heapUsed, nonHeapUsed;
	private int currentThreadCount;
	private long uptime;

	public BullyQuery() {
		// auto
	}
	public BullyQuery(String host) {
		this.host = host;
		this.created = new Date();
	}
	public Date getCreated() {
		return created;
	}
	public void setCreated(Date created) {
		this.created = created;
	}
	public String getHost() {
		return host;
	}
	public void setHost(String host) {
		this.host = host;
	}
	public double getLoad() {
		return load;
	}
	public void setLoad(double load) {
		this.load = load;
	}
	public double getHeapUsed() {
		return heapUsed;
	}
	public void setHeapUsed(double heapUsed) {
		this.heapUsed = heapUsed;
	}
	public double getNonHeapUsed() {
		return nonHeapUsed;
	}
	public void setNonHeapUsed(double nonHeapUsed) {
		this.nonHeapUsed = nonHeapUsed;
	}
	public int getCurrentThreadCount() {
		return currentThreadCount;
	}
	public void setCurrentThreadCount(int currentThreadCount) {
		this.currentThreadCount = currentThreadCount;
	}
	public long getUptime() {
		return uptime;
	}
	public void setUptime(long uptime) {
		this.uptime = uptime;
	}
}
