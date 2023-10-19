package fi.vm.yti.datamodel.api.v2.dto;

import java.util.Map;

public class ProcessingInfo {
	private String id;
	private Map<String, Object> params;
	public ProcessingInfo(String id, Map<String, Object> params) {
		super();
		this.id = id;
		this.params = params;
	}
	
	public ProcessingInfo() {}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Map<String, Object> getParams() {
		return params;
	}

	public void setParams(Map<String, Object> params) {
		this.params = params;
	}

	
}
