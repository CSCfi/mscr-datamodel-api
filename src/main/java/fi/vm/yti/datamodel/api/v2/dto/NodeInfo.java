package fi.vm.yti.datamodel.api.v2.dto;

public class NodeInfo {
	private String id;
	private String label;	
	private ProcessingInfo processing;
	private String uri;
	
	public NodeInfo() {}

	public NodeInfo(String id, String label, ProcessingInfo processing) {
		super();
		this.id = id;
		this.label = label;
		this.processing = processing;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getLabel() {
		return label;
	}

	public void setLabel(String label) {
		this.label = label;
	}

	public ProcessingInfo getProcessing() {
		return processing;
	}

	public void setProcessing(ProcessingInfo processing) {
		this.processing = processing;
	}

	public String getUri() {
		return uri;
	}

	public void setUri(String uri) {
		this.uri = uri;
	}
	
	
	
}
