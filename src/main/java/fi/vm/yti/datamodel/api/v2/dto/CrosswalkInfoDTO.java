package fi.vm.yti.datamodel.api.v2.dto;


import java.util.Set;

import org.apache.commons.lang3.builder.ToStringBuilder;

public class CrosswalkInfoDTO extends DataModelInfoDTO {
	
	private MSCRState state = MSCRState.DRAFT;
	private MSCRVisibility visibility = MSCRVisibility.PUBLIC;

	private CrosswalkFormat format;
	private String aggregationKey;
	private Set<FileMetadata> fileMetadata = Set.of();	
	private String sourceSchema;
	private String targetSchema;
	private Set<String> owner;

	public Set<String> getOwner() {
		return owner;
	}

	public void setOwner(Set<String> owner) {
		this.owner = owner;
	}

	public MSCRState getState() {
		return state;
	}

	public void setState(MSCRState state) {
		this.state = state;
	}
	
	public MSCRVisibility getVisibility() {
		return visibility;
	}
	public void setVisibility(MSCRVisibility visibility) {
		this.visibility = visibility;
	}
	
	public String getSourceSchema() {
		return sourceSchema;
	}
	public void setSourceSchema(String sourceSchema) {
		this.sourceSchema = sourceSchema;
	}
	public String getTargetSchema() {
		return targetSchema;
	}
	public void setTargetSchema(String targetSchema) {
		this.targetSchema = targetSchema;
	}
		
	private String PID;

	public String getPID() {
		return PID;
	}
	public void setPID(String pID) {
		PID = pID;
	}
	
	public CrosswalkFormat getFormat() {
		return format;
	}

	public void setFormat(CrosswalkFormat type) {
		this.format = type;
	}
	
    public String getAggregationKey() {
		return aggregationKey;
	}

	public void setAggregationKey(String aggregationKey) {
		this.aggregationKey = aggregationKey;
	}
	
	public Set<FileMetadata> getFileMetadata() {
		return fileMetadata;
	}

	public void setFileMetadata(Set<FileMetadata> fileMetadata) {
		this.fileMetadata = fileMetadata;
	}		

	@Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }	
}
