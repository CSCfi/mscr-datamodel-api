package fi.vm.yti.datamodel.api.v2.dto;

import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang3.builder.ToStringBuilder;

public class SchemaDTO extends CommonSchemaDTO implements MSCRCommonMetadata, InternalSchemaMetadataDTO {

	
    private Set<UUID> organizations = Set.of();
	
	public Set<UUID> getOrganizations() {
		return organizations;
	}
	public void setOrganizations(Set<UUID> organizations) {
		this.organizations = organizations;
	}
	
	
	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this);
	}



}
