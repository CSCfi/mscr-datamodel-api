package fi.vm.yti.datamodel.api.v2.dto.messaging;

public class AddSubscription {

	private final String action = "ADD";
	private String type;
	private String uri;
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
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
