package fi.vm.yti.datamodel.api.v2.dto.messaging;

import java.util.Map;

public class Resource {

	private String uri;
	private String application;
	private String type;
	private Map<String, String> prefLabel;
	public String getUri() {
		return uri;
	}
	public void setUri(String uri) {
		this.uri = uri;
	}
	public String getApplication() {
		return application;
	}
	public void setApplication(String application) {
		this.application = application;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public Map<String, String> getPrefLabel() {
		return prefLabel;
	}
	public void setPrefLabel(Map<String, String> prefLabel) {
		this.prefLabel = prefLabel;
	}
	
	
}
