package fi.vm.yti.datamodel.api.v2.dto;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.builder.ToStringBuilder;


public class SchemaInfoDTO extends DataModelInfoDTO implements MSCRCommonMetadata {
 
	private MSCRState state;
	private MSCRVisibility visibility;

	private Set<FileMetadata> fileMetadata = Set.of();
	private String PID;

	private SchemaFormat format;
	private String namespace;
	private String versionLabel;
	private String revisionOf;	

	private String aggregationKey;
	private List<String> hasRevisions;

	private List<Revision> revisions;
	private List<Variant> variants;
	private Map<String, List<Variant>> variants2;
	private Set<String> owner;
	private Set<OwnerDTO> ownerMetadata;
	private String sourceURL;
	private String customRoot;
	
	public String getCustomRoot() {
		return customRoot;
	}

	public void setCustomRoot(String customRoot) {
		this.customRoot = customRoot;
	}

	public String getSourceURL() {
		return sourceURL;
	}

	public void setSourceURL(String sourceURL) {
		this.sourceURL = sourceURL;
	}	
	

	public Set<OwnerDTO> getOwnerMetadata() {
		return ownerMetadata;
	}

	public void setOwnerMetadata(Set<OwnerDTO> owners) {
		this.ownerMetadata = owners;
	}

	public Set<String> getOwner() {
		return owner;
	}

	public void setOwner(Set<String> owner) {
		this.owner = owner;
	}
	
	public MSCRVisibility getVisibility() {
		return visibility;
	}
	public void setVisibility(MSCRVisibility visibility) {
		this.visibility = visibility;
	}
	
	public MSCRState getState() {
		return state;
	}

	public void setState(MSCRState state) {
		this.state = state;
	}	

	public String getPID() {
		return PID;
	}
	public void setPID(String pID) {
		PID = pID;
	}
	
	public SchemaFormat getFormat() {
		return format;
	}

	public void setFormat(SchemaFormat type) {
		this.format = type;
	}

	public String getNamespace() {
		return namespace;
	}

	public void setNamespace(String namespace) {
		this.namespace = namespace;
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
