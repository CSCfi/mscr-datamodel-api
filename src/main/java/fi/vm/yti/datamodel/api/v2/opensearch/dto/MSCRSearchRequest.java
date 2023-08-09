package fi.vm.yti.datamodel.api.v2.opensearch.dto;

import java.util.Set;
import java.util.UUID;

import fi.vm.yti.datamodel.api.v2.dto.MSCR;
import fi.vm.yti.datamodel.api.v2.dto.MSCRType;

public class MSCRSearchRequest extends BaseSearchRequest {

	private Set<MSCRType> type;
	private Set<UUID> organizations;
	private Set<String> format;
	private MSCR.SOURCE_TYPE sourceType;
	private Set<String> sourceSchemas;
	private Set<String> targetSchemas;

	private boolean includeFacets = false;
	private String prefDisplayLang;
	
	public Set<MSCRType> getType() {
		return type;
	}
	public void setType(Set<MSCRType> type) {
		this.type = type;
	}
	public Set<UUID> getOrganizations() {
		return organizations;
	}
	public void setOrganizations(Set<UUID> organizations) {
		this.organizations = organizations;
	}
	
	public Set<String> getFormat() {
		return format;
	}
	public void setFormat(Set<String> format) {
		this.format = format;
	}

	public MSCR.SOURCE_TYPE getSourceType() {
		return sourceType;
	}
	public void setSourceType(MSCR.SOURCE_TYPE sourceType) {
		this.sourceType = sourceType;
	}
	public Set<String> getSourceSchemas() {
		return sourceSchemas;
	}
	public void setSourceSchemas(Set<String> sourceSchemas) {
		this.sourceSchemas = sourceSchemas;
	}
	public Set<String> getTargetSchemas() {
		return targetSchemas;
	}
	public void setTargetSchemas(Set<String> targetSchemas) {
		this.targetSchemas = targetSchemas;
	}
	public boolean isIncludeFacets() {
		return includeFacets;
	}
	public void setIncludeFacets(boolean includeFacets) {
		this.includeFacets = includeFacets;
	}
	public String getPrefDisplayLang() {
		return prefDisplayLang;
	}
	public void setPrefDisplayLang(String prefDisplayLang) {
		this.prefDisplayLang = prefDisplayLang;
	}	
	
	
}

