package fi.vm.yti.datamodel.api.v2.dto;

public class NodeInfo {
	private String id;
	private String label;	
	private ProcessingInfo processing;
	private String value;
	
	public NodeInfo() {}

	public NodeInfo(String id, String label, ProcessingInfo processing, String value) {
		super();
		this.id = id;
		this.label = label;
		this.processing = processing;
		this.value = value;
	}
	
	

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
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
	
	
	
}
