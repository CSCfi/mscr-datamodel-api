package fi.vm.yti.datamodel.api.v2.dto;


import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.builder.ToStringBuilder;

public class CrosswalkInfoDTO extends DataModelInfoDTO implements MSCRCommonMetadata {
	
	private MSCRState state = MSCRState.DRAFT;
	private MSCRVisibility visibility = MSCRVisibility.PUBLIC;

	private String PID;
	
	private CrosswalkFormat format;
	private String aggregationKey;
	private Set<FileMetadata> fileMetadata = Set.of();	
	private String sourceSchema;
	private String targetSchema;
	private Set<String> owner;
	private List<MappingDTO> mappings;
	
	private String versionLabel;
	private String revisionOf;		
	private List<String> hasRevisions;
	private List<Revision> revisions;
	

	public List<MappingDTO> getMappings() {
		return mappings;
	}

	public void setMappings(List<MappingDTO> mappings) {
		this.mappings = mappings;
	}

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
	
	public String getVersionLabel() {
		return versionLabel;
	}

	public void setVersionLabel(String versionLabel) {
		this.versionLabel = versionLabel;
	}
	
	public String getRevisionOf() {
		return revisionOf;
	}

	public void setRevisionOf(String revisionOf) {
		this.revisionOf = revisionOf;
	}	
	
	public List<Revision> getRevisions() {
		return revisions;
	}

	public void setRevisions(List<Revision> revisions) {
		this.revisions = revisions;
	}

	
	public List<String> getHasRevisions() {
		return hasRevisions;
	}

	public void setHasRevisions(List<String> hasRevisions) {
		this.hasRevisions = hasRevisions;
	}
	

	@Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }	
}
