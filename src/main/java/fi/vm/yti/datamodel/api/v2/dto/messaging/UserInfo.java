package fi.vm.yti.datamodel.api.v2.dto.messaging;

import java.util.List;

public class UserInfo {
	private String id;
	private String subscriptionType;
	private List<Resource> resources;
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getSubscriptionType() {
		return subscriptionType;
	}
	public void setSubscriptionType(String subscriptionType) {
		this.subscriptionType = subscriptionType;
	}
	public List<Resource> getResources() {
		return resources;
	}
	public void setResources(List<Resource> resources) {
		this.resources = resources;
	}
	
}
