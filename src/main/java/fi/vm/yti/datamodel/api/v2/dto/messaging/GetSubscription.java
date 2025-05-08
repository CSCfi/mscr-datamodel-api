package fi.vm.yti.datamodel.api.v2.dto.messaging;

public class GetSubscription {

	private final String action = "GET";
	private String uri;
	public String getUri() {
		return uri;
	}
	public void setUri(String uri) {
		this.uri = uri;
	}
	public String getAction() {
		return action;
	}
	
	
}
