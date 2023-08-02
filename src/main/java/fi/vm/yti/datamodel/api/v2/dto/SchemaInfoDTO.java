package fi.vm.yti.datamodel.api.v2.dto;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.builder.ToStringBuilder;

import fi.vm.yti.datamodel.api.v2.mapper.SchemaMapper.SchemaRevision;
import fi.vm.yti.datamodel.api.v2.mapper.SchemaMapper.SchemaVariant;

public class SchemaInfoDTO extends DataModelInfoDTO {
  
	private SchemaFormat format;
	private String namespace;
	private String versionLabel;
	private String revisionOf;	

	private String aggregationKey;
	private List<String> hasRevisions;

	private List<SchemaRevision> revisions;
	private List<SchemaVariant> variants;
	private Map<String, List<SchemaVariant>> variants2;
	
	public Map<String, List<SchemaVariant>> getVariants2() {
		return variants2;
	}
	public void setVariants2(Map<String, List<SchemaVariant>> variants2) {
		this.variants2 = variants2;
	}

	private Set<FileMetadata> fileMetadata = Set.of();
	private String PID;

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
	
	public List<SchemaRevision> getRevisions() {
		return revisions;
	}

	public void setRevisions(List<SchemaRevision> revisions) {
		this.revisions = revisions;
	}

	public List<SchemaVariant> getVariants() {
		return variants;
	}

	public void setVariants(List<SchemaVariant> variants) {
		this.variants = variants;
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
