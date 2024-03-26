package fi.vm.yti.datamodel.api.v2.dto;

import java.util.List;

public class MappingDTO {

	protected String id;
	protected List<String> depends_on;
	protected List<NodeInfo> source;
	protected String sourceType; // type for all sources as a whole
	protected String sourceDescription; // desc for all sources as a whole
	protected String predicate;
	protected MappingFilterDTO filter;
	protected List<NodeInfo> target;
	protected String targetType;
	protected String targetDescription;	
	protected ProcessingInfo processing; 
	protected String notes;
	
	
	protected List<OneOfDTO> oneOf;

	public MappingDTO() {
		
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public List<String> getDepends_on() {
		return depends_on;
	}

	public void setDepends_on(List<String> depends_on) {
		this.depends_on = depends_on;
	}

	public List<NodeInfo> getSource() {
		return source;
	}

	public void setSource(List<NodeInfo> source) {
		this.source = source;
	}

	public String getSourceType() {
		return sourceType;
	}

	public void setSourceType(String sourceType) {
		this.sourceType = sourceType;
	}

	public String getSourceDescription() {
		return sourceDescription;
	}

	public void setSourceDescription(String sourceDescription) {
		this.sourceDescription = sourceDescription;
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

	public String getTargetType() {
		return targetType;
	}

	public void setTargetType(String targetType) {
		this.targetType = targetType;
	}

	public String getTargetDescription() {
		return targetDescription;
	}

	public void setTargetDescription(String targetDescription) {
		this.targetDescription = targetDescription;
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
		this.depends_on = depends_on;
		this.source = source;
		this.sourceType = sourceType;
		this.sourceDescription = sourceDescription;
		this.predicate = predicate;
		this.target = target;
		this.targetType = targetType;
		this.targetDescription = targetDescription;
		this.processing = processing;
	}

	public  MappingFilterDTO getFilter() {
		return filter;
	}

	public void setFilter( MappingFilterDTO filter) {
		this.filter = filter;
	}

	public List<OneOfDTO> getOneOf() {
		return oneOf;
	}

	public void setOneOf(List<OneOfDTO> oneOf) {
		this.oneOf = oneOf;
	}

	public String getNotes() {
		return notes;
	}

	public void setNotes(String notes) {
		this.notes = notes;
	}









	
}
