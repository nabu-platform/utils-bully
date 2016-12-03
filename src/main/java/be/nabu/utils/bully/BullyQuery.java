package be.nabu.utils.bully;

import java.util.Date;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "query")
public class BullyQuery {
	private String host;
	private Date created;

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
}
