package fi.vm.yti.datamodel.api.v2.dto;


import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.builder.ToStringBuilder;

import fi.vm.yti.datamodel.api.v2.dto.CrosswalkInfoDTO.CrosswalkSchemaInfo;

public class CrosswalkInfoDTO extends CommonCrosswalkDTO implements MSCRCommonMetadata, InternalCrosswalkMetadataDTO {
	
	public record CrosswalkSchemaInfo(String id, String handle, String name, String versionLabel, int versionIndex, String format, String originalFormat) {}
	
	private String ID;
	private String aggregationKey;
	private String handle;
	
	private Set<FileMetadata> fileMetadata = Set.of();	
	private List<GeneratedFileMetadata> generatedFileMetadata = List.of();	
	private CrosswalkSchemaInfo sourceSchemaInfo;
	private CrosswalkSchemaInfo targetSchemaInfo;

	private List<MappingDTO> mappings;	
	private Set<OwnerDTO> ownerMetadata;
	
	private String revisionOf;		
	private List<String> hasRevisions;
	private List<Revision> revisions;
    private Set<OrganizationDTO> organizations = Set.of();

	public Set<OrganizationDTO> getOrganizations() {
		return organizations;
	}

	public void setOrganizations(Set<OrganizationDTO> organizations) {
		this.organizations = organizations;
	}

	public String getHandle() {
		return handle;
	}

	public void setHandle(String handle) {
		this.handle = handle;
	}

	public CrosswalkSchemaInfo getSourceSchemaInfo() {
		return sourceSchemaInfo;
	}

	public void setSourceSchemaInfo(CrosswalkSchemaInfo sourceSchemaInfo) {
		this.sourceSchemaInfo = sourceSchemaInfo;
	}

	public CrosswalkSchemaInfo getTargetSchemaInfo() {
		return targetSchemaInfo;
	}

	public void setTargetSchemaInfo(CrosswalkSchemaInfo targetSchemaInfo) {
		this.targetSchemaInfo = targetSchemaInfo;
	}

	public Set<OwnerDTO> getOwnerMetadata() {
		return ownerMetadata;
	}

	public void setOwnerMetadata(Set<OwnerDTO> owners) {
		this.ownerMetadata = owners;
	}
	
	public List<MappingDTO> getMappings() {
		return mappings;
	}

	public void setMappings(List<MappingDTO> mappings) {
		this.mappings = mappings;
	}



	public String getID() {
		return ID;
	}
	public void setID(String pID) {
		ID = pID;
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
	

	public List<GeneratedFileMetadata> getGeneratedFileMetadata() {
		return generatedFileMetadata;
	}

	public void setGeneratedFileMetadata(List<GeneratedFileMetadata> generatedFileMetadata) {
		this.generatedFileMetadata = generatedFileMetadata;
	}

	@Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }	
}
