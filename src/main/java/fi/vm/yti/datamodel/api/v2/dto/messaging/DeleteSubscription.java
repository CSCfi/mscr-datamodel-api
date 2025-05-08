package fi.vm.yti.datamodel.api.v2.dto.messaging;

public class DeleteSubscription {

	private final String action = "DELETE";
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
