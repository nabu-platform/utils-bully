package be.nabu.utils.bully;

import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "overview")
public class BullyQueryOverview {
	private List<BullyQueryList> lists;

	public List<BullyQueryList> getLists() {
		return lists;
	}

	public void setLists(List<BullyQueryList> lists) {
		this.lists = lists;
	}
	
}
