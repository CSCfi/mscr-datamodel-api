package fi.vm.yti.datamodel.api.v2.dto.messaging;

public class SubscriptionResponse {

	private String uri;
	private String type;
	private String application;
	
	
	public String getApplication() {
		return application;
	}
	public void setApplication(String application) {
		this.application = application;
	}
	public String getUri() {
		return uri;
	}
	public void setUri(String uri) {
		this.uri = uri;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	
	
}
