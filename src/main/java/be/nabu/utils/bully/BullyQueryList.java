package be.nabu.utils.bully;

import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "queries")
public class BullyQueryList {
	
	private String host;
	private List<BullyQuery> queries;

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public List<BullyQuery> getQueries() {
		return queries;
	}

	public void setQueries(List<BullyQuery> queries) {
		this.queries = queries;
	}
	
}
