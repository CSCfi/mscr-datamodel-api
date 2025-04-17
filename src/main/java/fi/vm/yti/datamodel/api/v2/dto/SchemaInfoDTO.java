package fi.vm.yti.datamodel.api.v2.dto;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.builder.ToStringBuilder;


public class SchemaInfoDTO extends CommonSchemaDTO implements MSCRCommonMetadata, InternalSchemaMetadataDTO {
 
	private Set<FileMetadata> fileMetadata = Set.of();
	private String revisionOf;	
	private List<String> hasRevisions;

	private List<Revision> revisions;
	private List<Variant> variants;
	private Map<String, List<Variant>> variants2;
	private Set<OwnerDTO> ownerMetadata;
	private String customRoot;
	
	private String ID;
	private String aggregationKey;
	private String handle;

    private Set<OrganizationDTO> organizations = Set.of();

	public Set<OrganizationDTO> getOrganizations() {
		return organizations;
	}

	public void setOrganizations(Set<OrganizationDTO> organizations) {
		this.organizations = organizations;
	}

	
	public String getID() {
		return ID;
	}

	public void setID(String ID) {
		this.ID = ID;
	}

	public String getAggregationKey() {
		return aggregationKey;
	}

	public void setAggregationKey(String aggregationKey) {
		this.aggregationKey = aggregationKey;
	}

	public String getHandle() {
		return handle;
	}

	public void setHandle(String handle) {
		this.handle = handle;
	}

	public String getCustomRoot() {
		return customRoot;
	}

	public void setCustomRoot(String customRoot) {
		this.customRoot = customRoot;
	}

	public Set<OwnerDTO> getOwnerMetadata() {
		return ownerMetadata;
	}

	public void setOwnerMetadata(Set<OwnerDTO> owners) {
		this.ownerMetadata = owners;
	}
	
	public String getRevisionOf() {
		return revisionOf;
	}

	public void setRevisionOf(String revisionOf) {
		this.revisionOf = revisionOf;
	}
	
	public Set<FileMetadata> getFileMetadata() {
		return fileMetadata;
	}

	public void setFileMetadata(Set<FileMetadata> fileMetadata) {
		this.fileMetadata = fileMetadata;
	} 
	
	public List<Revision> getRevisions() {
		return revisions;
	}

	public void setRevisions(List<Revision> revisions) {
		this.revisions = revisions;
	}

	public List<Variant> getVariants() {
		return variants;
	}

	public void setVariants(List<Variant> variants) {
		this.variants = variants;
	}
	public List<String> getHasRevisions() {
		return hasRevisions;
	}

	public void setHasRevisions(List<String> hasRevisions) {
		this.hasRevisions = hasRevisions;
	}

	public Map<String, List<Variant>> getVariants2() {
		return variants2;
	}
	public void setVariants2(Map<String, List<Variant>> variants2) {
		this.variants2 = variants2;
	}


	@Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }

}
