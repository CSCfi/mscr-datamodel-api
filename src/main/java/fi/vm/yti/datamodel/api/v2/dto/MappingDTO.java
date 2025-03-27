package fi.vm.yti.datamodel.api.v2.dto;

import java.util.List;

public class MappingDTO {

	protected String id;
	protected List<NodeInfo> source;
	protected String predicate;
	protected List<NodeInfo> target;
	protected ProcessingInfo processing; 
	protected String notes;
	protected String confidence;
	protected String justification;
	
	
	public MappingDTO() {
		
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public List<NodeInfo> getSource() {
		return source;
	}

	public void setSource(List<NodeInfo> source) {
		this.source = source;
	}

	public String getPredicate() {
		return predicate;
	}

	public void setPredicate(String predicate) {
		this.predicate = predicate;
	}

	public List<NodeInfo> getTarget() {
		return target;
	}

	public void setTarget(List<NodeInfo> target) {
		this.target = target;
	}

	public ProcessingInfo getProcessing() {
		return processing;
	}

	public void setProcessing(ProcessingInfo processing) {
		this.processing = processing;
	}

	public MappingDTO(String id, List<String> depends_on, List<NodeInfo> source, String sourceType,
			String sourceDescription, String predicate, List<NodeInfo> target, String targetType,
			String targetDescription, ProcessingInfo processing) {
		super();
		this.id = id;
		this.source = source;
		this.predicate = predicate;
		this.target = target;
		this.processing = processing;
	}

	public String getNotes() {
		return notes;
	}

	public void setNotes(String notes) {
		this.notes = notes;
	}

	public String getJustification() {
		return justification;
	}

	public void setJustification(String justification) {
		this.justification = justification;
	}

	public String getConfidence() {
		return confidence;
	}

	public void setConfidence(String confidence) {
		this.confidence = confidence;
	}









	
}
